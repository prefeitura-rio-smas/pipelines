"""Regressões das regras SQL do Centro POP, executáveis sem conexão ao BigQuery.

Os testes executam expressões e joins do modelo em SQLite. A execução nativa
da extração HTML é coberta pelo teste SQL do dbt.
"""

import json
from pathlib import Path
import re
import sqlite3
from types import SimpleNamespace
import unittest

from jinja2 import Environment

ROOT = Path(__file__).resolve().parents[1]
MACROS = ROOT / "queries/macros"
MART_PATH = (
    ROOT / "queries/models/marts/centro_pop/mart_planilha_unificada_centro_pop.sql"
)


def render_sql(path):
    env = Environment(autoescape=False)  # noqa: S701 - Renderização SQL local; escape HTML altera os operadores SQL.
    env.globals.update(
        ref=lambda name: name,
        config=lambda **kwargs: "",
        var=lambda name, default=None: default,
        dbt_utils=SimpleNamespace(generate_surrogate_key=lambda fields: "'grupo'"),
        calc_idade=lambda *args: "0",
        extrair_ultima_atualizacao=lambda *args: "NULL",
    )
    paths = [
        MACROS / "acolherio/mapa_colunas_categoria.sql",
        MACROS / "acolherio/extrair_campos_html_evolucao.sql",
        MACROS / "acolherio/extrair_formulario.sql",
        MACROS / "acolherio/evidencia_saude.sql",
        MACROS / "registro_ate_evento.sql",
    ]
    module = env.from_string("\n".join(p.read_text() for p in paths)).module
    env.globals.update(
        {
            name: getattr(module, name)
            for name in dir(module)
            if not name.startswith("_")
        }
    )
    return env.from_string(path.read_text()).render()


def cte(sql, name):
    return re.search(r"\b" + name + r" as \(\n(.*?)\n\),", sql, re.S).group(1)


def sqlite_sql(sql):
    sql = re.sub(r"\br'([^']*)'", r"'\1'", sql)
    sql = re.sub(
        r"safe_cast\(([^()]*) as (?:string|int64|date)\)",
        r"cast(\1 as text)",
        sql,
        flags=re.I,
    )
    # All input dates in these fixtures use ISO format.
    sql = (
        sql.replace("safe_cast", "cast")
        .replace(" as int64)", " as integer)")
        .replace(" as string)", " as text)")
        .replace(" as date)", " as text)")
    )
    return sql


class CentroPopRegressoes(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.mart = render_sql(MART_PATH)
        cls.db = sqlite3.connect(":memory:")
        cls.db.create_function(
            "regexp_contains",
            2,
            lambda value, pattern: (
                None if value is None else bool(re.search(pattern, value))
            ),
        )
        cls.db.create_function(
            "safe_int",
            1,
            lambda value: (
                int(value)
                if value is not None and re.fullmatch(r"[+-]?\d+", value)
                else None
            ),
        )

    @classmethod
    def tearDownClass(cls):
        cls.db.close()

    def scalar(self, expression, value):
        return self.db.execute(
            f"select {sqlite_sql(expression)} from (select ? as valor)", (value,)  # noqa: S608 - SQL do repositório e fixtures locais; sem entrada externa.
        ).fetchone()[0]

    def test_escolaridade_e_serie_sao_dominios_distintos(self):
        sql = render_sql(ROOT / "queries/models/intermediate/core/int_usuarios.sql")
        expression = re.search(
            r"(CASE\s+WHEN sm\.escolaridade.*?END)\s+as escolaridade_indice", sql, re.S
        ).group(1)
        actual = self.db.execute(
            f"with sm(serie_escolar, escolaridade) as (values ('12','06')) select {expression} from sm"  # noqa: S608 - SQL do repositório e fixtures locais; sem entrada externa.
        ).fetchone()[0]
        self.assertEqual(actual, "Nível médio completo")
        expression = re.search(
            r"(case\s+when lower\(trim\(u.flag_frequenta_escola\)\).*?end) as ano_cursando",
            self.mart.split("end as flag_estuda,")[1],
            re.S,
        ).group(1)
        actual = self.db.execute(
            f"with u(flag_frequenta_escola, serie_escolar) as (values ('S','12')) select {expression} from u"  # noqa: S608 - SQL do repositório e fixtures locais; sem entrada externa.
        ).fetchone()[0]
        self.assertEqual(actual, "Ensino Médio - 1° Ano")

    def test_todos_os_tipos_de_deficiencia_sao_preservados(self):
        expression = (
            self.mart.split("end as flag_deficiencia,", 1)[1]
            .split("as tipo_deficiencia,", 1)[0]
            .strip()
        )
        array_expr = re.search(
            r"array_to_string\(array\((.*?)\), ', '\)", expression, re.S
        ).group(0)
        select_sql = re.search(
            r"array_to_string\(array\((.*?)\), ', '\)", expression, re.S
        ).group(1)
        select_sql = select_sql.replace(
            "from unnest(u.deficiencia) as d", "from json_each(?) as d"
        )
        select_sql = select_sql.replace(
            "d.descricao", "json_extract(d.value, '$.descricao')"
        ).replace("d.codigo", "json_extract(d.value, '$.codigo')")
        expression = expression.replace(
            array_expr, f"(select group_concat(descricao, ', ') from ({select_sql}))"  # noqa: S608 - SQL do repositório e fixtures locais; sem entrada externa.
        )
        fixture = [
            {"codigo": "1", "descricao": "Deficiência auditiva total"},
            {"codigo": "4", "descricao": "Deficiência motora"},
            {"codigo": "4", "descricao": "Deficiência motora"},
            {"codigo": "99", "descricao": None},
        ]
        query = "select " + expression
        self.assertEqual(
            self.db.execute(query, (json.dumps(fixture),)).fetchone()[0],
            "99, Deficiência auditiva total, Deficiência motora",
        )
        for value in [None, "[]"]:
            self.assertEqual(
                self.db.execute(query, (value,)).fetchone()[0], "Não Informado"
            )

    def test_flags_preservam_ausencia_e_resposta_invalida(self):
        env = Environment(autoescape=False)  # noqa: S701 - Renderização SQL local; escape HTML altera os operadores SQL.
        macro = env.from_string(
            (MACROS / "acolherio/mapa_colunas_categoria.sql").read_text()
        ).module
        for value, expected in [
            (None, None),
            ("", None),
            ("undefined", None),
            (" s ", "Sim"),
            ("N", "Não"),
        ]:
            with self.subTest(flag="cadunico", value=value):
                self.assertEqual(
                    self.scalar(macro.map_flag_cadunico("valor"), value), expected
                )
        for value, expected in [
            (None, None),
            (" ", None),
            ("undefined", None),
            ("5", "Sim"),
            ("1", "Não"),
        ]:
            with self.subTest(flag="situacao_rua", value=value):
                self.assertEqual(
                    self.scalar(macro.map_flag_situacao_rua("valor"), value), expected
                )

    def test_saude_desconhecida_nao_e_evidencia_positiva(self):
        macro = (
            Environment(autoescape=False)  # noqa: S701 - Renderização SQL local; escape HTML altera os operadores SQL.
            .from_string((MACROS / "acolherio/evidencia_saude.sql").read_text())
            .module
        )
        for value, expected in [
            (None, None),
            ("", None),
            ("undefined", None),
            (" Não Informado ", None),
            ("Não sabe", None),
            ("não se aplica", None),
            ("N", 0),
            ("Não", 0),
            ("Diabetes", 1),
            ("Clínica da Família", 1),
        ]:
            with self.subTest(value=value):
                self.assertEqual(
                    self.scalar(macro.evidencia_saude("valor"), value), expected
                )

    def test_questionario_isola_template_e_modulo(self):
        sql = cte(self.mart, "questionario_situacao_usuario")
        fixtures = """with raw_evolucoes_questionario(id_prontuario,id_evolucao,id_template,id_modulo) as
            (values (42,7,3,1),(43,8,3,null)),
            raw_evolucoes_questionario_lista(id_evolucao,id_template,id_modulo,id_questao,data_resposta,resposta) as
            (values (7,3,1,18,'2026-09-01','Correta'),(7,99,1,18,'2026-09-01','Outro template'),
            (7,3,2,18,'2026-09-01','Outro módulo'),(7,3,null,18,'2026-09-01','Módulo ausente'),
            (8,3,null,18,'2026-09-01','Ambos sem módulo')) """
        self.assertEqual(
            self.db.execute(fixtures + sql).fetchall(),
            [
                (42, 7, "2026-09-01", "Correta"),
                (43, 8, "2026-09-01", "Ambos sem módulo"),
            ],
        )

    def test_evolucao_familiar_com_paciente_entra_no_perfil_saude(self):
        sql = cte(self.mart, "evolucoes_cadastro")
        sql = re.sub(
            r"concat\(e.origem_modulo, \':\', cast\(e.id_evolucao as string\)\)",
            "e.origem_modulo || ':' || e.id_evolucao",
            sql,
        )
        fixtures = """with fct_evolucoes(codigo_abrangencia,descricao_evolucao,data_evolucao,id_usuario_sk,id_paciente_familia,origem_modulo,id_evolucao,data_cancelamento) as
            (values (1,'HTML','2026-09-01',null,42,'familia',7,null),
            (1,'Cancelado','2026-09-01',null,42,'familia',8,'2026-09-02'),
            (1,'Sem paciente','2026-09-01',null,null,'familia',9,null)),
            dim_usuarios(id_usuario_sk,id_usuario) as (values ('sk42',42)) """
        rows = self.db.execute(fixtures + sql).fetchall()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][3:], (42, "familia:7"))

    def test_ultimo_tecnico_busca_historico_e_respeita_unidade_e_corte(self):
        sql = sqlite_sql(cte(self.mart, "ultimo_atendimento_tecnico"))
        fixtures = """with usuarios_mes(id_usuario_unidade_mes,id_usuario,id_unidade,data_referencia) as
            (values ('set',42,1,'2026-09-30'),('out',42,1,'2026-10-31')),
            atendimentos(id_usuario,id_unidade,tipo_atendimento,data_atendimento) as
            (values (42,1,'Atendimento Técnico','2026-08-10'),
            (42,1,'Atendimento Recepção','2026-09-20'),
            (42,2,'Atendimento Técnico','2026-09-25'),
            (42,1,'Atendimento Técnico','2026-10-02'),
            (43,1,'Atendimento Técnico','2026-09-27')) """
        self.assertEqual(
            dict(self.db.execute(fixtures + sql).fetchall()),
            {"set": "2026-08-10", "out": "2026-10-02"},
        )

    def test_saude_historica_preserva_sim_e_exclui_futuro(self):
        sql = sqlite_sql(cte(self.mart, "situacao_saude_historico_mes")).replace(
            "logical_or(", "max("
        )
        fixtures = """with usuarios_mes(id_usuario_unidade_mes,id_usuario,data_referencia) as
            (values ('positivo',1,'2026-09-30'),('negativo',2,'2026-09-30'),('desconhecido',3,'2026-09-30')),
            situacao_saude(id_paciente,situacao_saude,data_evolucao) as
            (values (1,'Diabetes','2026-08-01'),(1,'Não','2026-09-20'),
            (2,'Não','2026-09-20'),(2,'Diabetes','2026-10-01'),
            (3,'Não Informado','2026-09-20')) """
        self.assertEqual(
            dict(self.db.execute(fixtures + sql).fetchall()),
            {"positivo": 1, "negativo": 0, "desconhecido": None},
        )

    def test_classificacao_tecnica_considera_todos_os_cbos(self):
        body = cte(self.mart, 'atendimentos_candidatos')
        expression = re.search(r'(case.*?end) as tipo_atendimento', body, re.S).group(1)
        expression = expression.replace(
            'from unnest(coalesce(dp.codigos_cbo, [dp.cbo_principal_codigo])) as codigo_cbo',
            'from json_each(coalesce(dp.codigos_cbo, json_array(dp.cbo_principal_codigo))) as cbo',
        ).replace('trim(codigo_cbo)', 'trim(cbo.value)')
        fixture = """with a(tipo_atendimento_descricao) as (values ('Centro POP - Recepção')),
            dp(codigos_cbo,cbo_principal_codigo,nome,cbo_principal_descricao) as
            (values (?,?,null,null)) """
        query = fixture + 'select ' + expression + ' from a cross join dp'  # noqa: S608 - Expressão SQL do repositório; valores parametrizados.
        for codes, principal, expected in [
            (['123456', '251605'], '123456', 'Atendimento Técnico'),
            (['123456'], '123456', 'Atendimento Recepção'),
            (None, '251605', 'Atendimento Técnico'),
        ]:
            with self.subTest(codes=codes):
                data = json.dumps(codes) if codes is not None else None
                self.assertEqual(self.db.execute(query, (data, principal)).fetchone()[0], expected)

    def test_compartilhado_preserva_principal_nulo_e_deduplica_apos_cast(self):
        sql = render_sql(ROOT / "queries/models/intermediate/core/fct_atendimentos.sql")
        body = cte(sql, "uniao_atendimentos")
        cast_expr = re.search(r"select distinct (.*?)\n", body).group(1)
        cast_expr = cast_expr.replace(
            "safe_cast(trim(x) as int64)", "safe_int(trim(x))"
        )
        list_filter = re.search(r'where\s+(.*?)\n\s*\)\) as prof_id', body, re.S).group(1)
        list_filter = list_filter.replace('safe_cast(trim(x) as int64)', 'safe_int(trim(x))')
        predicate = body.split("where prof_id is distinct from ", 1)[1].strip()
        predicate = "prof_id is distinct from " + predicate.replace(
            "safe_cast(trim(cast(uniao_atendimentos_base.id_profissional as string)) as int64)",
            "safe_int(trim(cast(uniao_atendimentos_base.id_profissional as text)))",
        )
        sql = f"""with uniao_atendimentos_base(id_profissional) as (values (null)),
            valores(x) as (values ('0012'),('12'),(' 12 '),('34'),('invalid'),(''),('000')),
            compartilhados as (select distinct {cast_expr} as prof_id from valores where {list_filter})
            select prof_id from compartilhados cross join uniao_atendimentos_base where {predicate} order by prof_id"""  # noqa: S608 - SQL do repositório e fixtures locais.
        self.assertEqual(self.db.execute(sql).fetchall(), [(12,), (34,)])
        self.assertEqual(
            self.db.execute(sql.replace("(values (null))", "(values (12))")).fetchall(),
            [(34,)],
        )


if __name__ == "__main__":
    unittest.main()
