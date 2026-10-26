# ruff: noqa: S608 - queries assembled exclusively from local synthetic fixtures
"""Regressões com dados sintéticos, executando o SQL real dos novos modelos.

RUN_BIGQUERY_TESTS=1 .venv/bin/python -m unittest discover -s queries/tests/planilha_unificada \
    -p 'test_modelos.py' -v

Os testes não leem tabelas externas nem materializam dados. Requerem credenciais
BigQuery e usam BIGQUERY_TEST_PROJECT (padrão: rj-smas-dev) para executar consultas.
"""

import json
import os
from pathlib import Path
import re
import unittest

from jinja2 import Environment, StrictUndefined

QUERIES = Path(__file__).resolve().parents[2]


def sql_literal(value):
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, list):
        return "[" + ", ".join(sql_literal(item) for item in value) + "]"
    escaped = (
        value.replace("\\", "\\\\")
        .replace("'", "\\'")
        .replace("\n", "\\n")
        .replace("\r", "\\r")
        .replace("\t", "\\t")
    )
    return "'" + escaped + "'"


def fixture_rows(schema, rows):
    """Relação tipada para que NULL não seja inferido como INT64 pelo BigQuery."""
    return " union all ".join(
        "select "
        + ", ".join(
            f"cast({sql_literal(row.get(name))} as {dtype}) as {name}"
            for name, dtype in schema.items()
        )
        for row in rows
    )


@unittest.skipUnless(
    os.getenv("RUN_BIGQUERY_TESTS") == "1", "Consultas sintéticas BigQuery opcionais"
)
class PlanilhaUnificadaTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from google.cloud import bigquery

        cls.client = bigquery.Client(
            project=os.getenv("BIGQUERY_TEST_PROJECT", "rj-smas-dev")
        )

    def consultar_modelo(self, relative_path, fixtures):
        # Templates SQL locais; escape HTML alteraria operadores e expressões SQL.
        env = Environment(undefined=StrictUndefined, autoescape=False)  # noqa: S701
        fixtures = dict(fixtures)
        fixtures.setdefault(
            "raw_familias_motivos_saida",
            "select cast(null as int64) id_motivo_saida, cast(null as string) descricao_motivo_saida from unnest([1]) where false",
        )
        fixtures.setdefault(
            "dim_usuarios",
            "select 1 id_usuario, 'Pessoa A' nome union all select 2, 'Pessoa B'",
        )
        fixtures.setdefault(
            "dim_profissionais",
            "select cast(null as int64) id_profissional, cast(null as string) nome from unnest([1]) where false",
        )
        fixtures.setdefault(
            "raw_operadores",
            "select cast(null as int64) id_login, cast(null as string) nome_operador from unnest([1]) where false",
        )
        fixtures.setdefault(
            "raw_atividades_grupo",
            "select cast(null as int64) id_atividade, cast(null as string) nome_atividade from unnest([1]) where false",
        )
        if relative_path.endswith("int_pu_formularios.sql"):
            fixtures["int_pu_evolucoes"] = (
                "select e.*, cast(null as string) nome_profissional, cast(null as string) nome_atividade from ("  # noqa: S608
                + fixtures["int_pu_evolucoes"]
                + ") e"
            )
        services = {
            "cras": "dim_cras",
            "creas": "dim_creas",
            "centro_pop": "dim_centro_pop",
            "outros": "dim_outros_equipamentos",
        }

        def reference(name):
            if name in services.values():
                service = next(k for k, v in services.items() if v == name)
                return "(" + dimension_macros.pu_dimensao_sql(service) + ")"
            return f"({fixtures[name]})"

        env.globals.update(
            ref=reference,
            pu_ref=reference,
            execute=False,
            config=lambda **kwargs: "",
            var=lambda name, default=None: default,
            pu_servicos=lambda: services,
        )

        normalizacao = QUERIES / "macros/decodificar_entidades_html.sql"
        env.globals["decodificar_entidades_html"] = env.from_string(
            normalizacao.read_text()
        ).module.decodificar_entidades_html
        env.globals["texto_html_legivel"] = env.from_string(
            (QUERIES / "macros/texto_html_legivel.sql").read_text()
        ).module.texto_html_legivel
        fixtures.setdefault(
            "dim_unidades",
            "select 10 id_unidade, 'Unidade A' nome_unidade, 'CRAS' tipo_unidade union all select 20, 'Unidade A', 'CRAS'",
        )
        env.globals["normalizar_hora_atendimento"] = env.from_string(
            (QUERIES / "macros/normalizar_hora_atendimento.sql").read_text()
        ).module.normalizar_hora_atendimento
        categorias = env.from_string(
            (QUERIES / "macros/acolherio/mapa_colunas_categoria.sql").read_text()
        ).module
        env.globals["map_nome_projeto_social"] = categorias.map_nome_projeto_social
        env.globals["map_nome_servico_assistencial"] = (
            categorias.map_nome_servico_assistencial
        )
        macro = QUERIES / "macros/acolherio/extrair_campos_html_evolucao.sql"
        env.globals["extrair_campos_html_evolucao"] = env.from_string(
            macro.read_text()
        ).module.extrair_campos_html_evolucao
        form_macros = env.from_string(
            (QUERIES / "macros/planilha_unificada/formularios.sql").read_text()
        ).module
        env.globals["pu_formularios_sql"] = form_macros.pu_formularios_sql
        if relative_path.endswith("planilha_unificada.sql"):
            # Output fixtures already contain extracted forms; dimension tests exercise the real parser.
            env.globals["pu_formularios_sql"] = lambda source: (
                "select * from " + reference("int_pu_formularios")
            )
            if "tipo_servico" not in fixtures["int_pu_atendimentos"]:
                fixtures["int_pu_atendimentos"] = (
                    "select p.id_usuario, array(select as struct a.*, 'cras' as tipo_servico from unnest(p.atendimentos) a) atendimentos from ("
                    + fixtures["int_pu_atendimentos"]
                    + ") p"
                )
            if "tipo_servico" not in fixtures["int_pu_evolucoes"]:
                fixtures["int_pu_evolucoes"] = (
                    "select e.*, 'cras' as tipo_servico from ("
                    + fixtures["int_pu_evolucoes"]
                    + ") e"
                )
        env.globals["pu_tipo_servico"] = lambda expression: (
            "case "
            + expression
            + " when 'CRAS' then 'cras' when 'CREAS' then 'creas' when 'Centro POP' then 'centro_pop' else 'outros' end"
        )
        macro_sql = (QUERIES / "macros/planilha_unificada/dimensoes.sql").read_text()
        macro_sql = re.sub(
            r"{% macro pu_(servicos|ref)\(.*?{% endmacro %}", "", macro_sql, flags=re.S
        )
        dimension_macros = env.from_string(macro_sql).module
        env.globals.update(
            pu_tipo_servico=dimension_macros.pu_tipo_servico,
            pu_dimensoes_sql=dimension_macros.pu_dimensoes_sql,
            pu_dimensao_sql=dimension_macros.pu_dimensao_sql,
            pu_contagens_servico=dimension_macros.pu_contagens_servico,
        )
        sql = env.from_string((QUERIES / relative_path).read_text()).render()
        # SQL vem apenas dos modelos versionados e fixtures sintéticas deste teste.
        query = (
            f"select to_json_string(resultado) as resultado from ({sql}) as resultado"  # noqa: S608
        )
        return sorted(
            (
                json.loads(row.resultado)
                for row in self.client.query(query).result(timeout=60)
            ),
            key=lambda row: (
                row["usuario"]["id_usuario"] if "usuario" in row else row["id_usuario"]
            ),
        )

    def test_atendimentos_preservam_modulo_unidade_data_cancelamento_e_profissionais(
        self,
    ):
        schema = {
            "id_usuario": "int64",
            "id_atendimento_modulo": "string",
            "id_atendimento": "int64",
            "origem_modulo": "string",
            "id_unidade": "int64",
            "id_tipo_atendimento": "int64",
            "tipo_atendimento_descricao": "string",
            "data_atendimento": "date",
            "hora_atendimento": "string",
            "flag_cancelado": "string",
            "id_profissional": "int64",
            "id_profissional_sk": "string",
        }
        evento = dict(
            id_usuario=1,
            id_atendimento_modulo="7u",
            id_atendimento=7,
            origem_modulo="usuario",
            id_unidade=10,
            id_tipo_atendimento=1,
            tipo_atendimento_descricao="Atendimento",
            data_atendimento="2026-01-02",
            hora_atendimento="09:00",
            id_profissional=100,
            id_profissional_sk="p100",
        )
        outro = dict(
            evento,
            id_atendimento_modulo="7f",
            origem_modulo="familia",
            id_unidade=20,
            data_atendimento="2026-02-02",
            flag_cancelado="N",
            id_profissional=None,
            id_profissional_sk=None,
        )
        fixtures = {
            "fct_atendimentos": fixture_rows(
                schema,
                [
                    evento,
                    evento,
                    dict(evento, id_profissional=101, id_profissional_sk="p101"),
                    outro,
                    dict(evento, id_atendimento_modulo="8u", flag_cancelado="S"),
                    dict(evento, id_atendimento_modulo="9u", flag_cancelado=" s "),
                    dict(
                        evento,
                        id_usuario=2,
                        id_atendimento_modulo="10u",
                        flag_cancelado="S",
                    ),
                ],
            ),
            "dim_profissionais": fixture_rows(
                {
                    "id_profissional_sk": "string",
                    "nome": "string",
                    "codigos_cbo": "array<string>",
                    "descricoes_cbo": "array<string>",
                    "cbo_principal_codigo": "string",
                    "cbo_principal_descricao": "string",
                },
                [
                    dict(
                        id_profissional_sk="p100",
                        nome="Profissional A",
                        codigos_cbo=["2516"],
                    ),
                    dict(
                        id_profissional_sk="p101",
                        nome="Profissional B",
                        codigos_cbo=["4110"],
                    ),
                ],
            ),
        }
        resultado = self.consultar_modelo(
            "models/intermediate/planilha_unificada/atendimentos/int_pu_atendimentos.sql",
            fixtures,
        )
        self.assertEqual(len(resultado), 1)
        eventos = resultado[0]["atendimentos"]
        self.assertEqual(len(eventos), 2)
        self.assertEqual([e["id_atendimento_modulo"] for e in eventos], ["7u", "7f"])
        self.assertEqual(
            [p["id_profissional"] for p in eventos[0]["profissionais"]], [100, 101]
        )
        self.assertEqual(eventos[1]["profissionais"], [])
        self.assertEqual(eventos[1]["flag_cancelado"], "N")
        self.assertEqual(eventos[1]["id_unidade"], 20)
        self.assertEqual(eventos[1]["data_atendimento"], "2026-02-02")

    def test_conflitos_de_evento_nao_sao_ocultados(self):
        fixtures = {
            "fct_atendimentos": """select 1 id_usuario, '7u' id_atendimento_modulo, 7 id_atendimento,
                'usuario' origem_modulo, unidade id_unidade, 1 id_tipo_atendimento,
                'Atendimento' tipo_atendimento_descricao, date '2026-01-02' data_atendimento,
                '09:00' hora_atendimento, cast(null as string) flag_cancelado,
                cast(null as int64) id_profissional, cast(null as string) id_profissional_sk
                from unnest([10, 20]) unidade""",
            "dim_profissionais": """select cast(null as string) id_profissional_sk,
                cast(null as string) nome, array<string>[] codigos_cbo, array<string>[] descricoes_cbo,
                cast(null as string) cbo_principal_codigo, cast(null as string) cbo_principal_descricao
                from unnest([1]) where false""",
        }
        resultado = self.consultar_modelo(
            "models/intermediate/planilha_unificada/atendimentos/int_pu_atendimentos.sql",
            fixtures,
        )
        self.assertEqual(
            [e["id_unidade"] for e in resultado[0]["atendimentos"]], [10, 20]
        )

    def test_documentos_ambiguos_e_ausencia_nao_viram_valores_inventados(self):
        fixtures = {
            "dim_usuarios": fixture_rows(
                {"id_usuario": "int64", "cpf": "string"},
                [
                    dict(id_usuario=1, cpf="111.111.111-11"),
                    dict(id_usuario=2, cpf="22222222222"),
                    dict(id_usuario=3, cpf="33333333333"),
                ],
            ),
            "raw_documento_pessoa": fixture_rows(
                {
                    "cpf": "string",
                    "id_familia": "string",
                    "rg": "string",
                    "id_certidao_civil": "string",
                    "id_termi_matricula_certidao": "string",
                    "id_membro_familia": "string",
                },
                [
                    dict(
                        cpf="11111111111",
                        id_familia="10",
                        rg="00123",
                        id_certidao_civil="1",
                        id_termi_matricula_certidao="cert",
                        id_membro_familia="a",
                    ),
                    dict(
                        cpf="11111111111",
                        id_familia="11",
                        rg="00456",
                        id_certidao_civil="1",
                        id_termi_matricula_certidao="cert",
                        id_membro_familia="b",
                    ),
                    dict(cpf="22222222222", id_familia="20"),
                ],
            ),
            "raw_identificacao_membro": fixture_rows(
                {"id_membro_familia": "string", "nis": "string"},
                [
                    dict(id_membro_familia="a", nis="nis-a"),
                    dict(id_membro_familia="b", nis="nis-b"),
                ],
            ),
            "raw_identificacao_controle": fixture_rows(
                {
                    "id_familia": "string",
                    "data_atualizacao": "date",
                    "data_alteracao": "date",
                    "data_limite_catastro_atual": "date",
                },
                [
                    dict(id_familia="10", data_atualizacao="2026-01-01"),
                    dict(id_familia="20", data_atualizacao="2026-02-01"),
                ],
            ),
        }
        resultado = self.consultar_modelo(
            "models/intermediate/planilha_unificada/documentos/int_pu_documentos_cadunico.sql",
            fixtures,
        )
        self.assertEqual([r["id_usuario"] for r in resultado], [1, 2])
        ambiguo = resultado[0]["documentos_cadunico"]
        self.assertIsNone(ambiguo["id_familia_cadunico"])
        self.assertIsNone(ambiguo["nis"])
        self.assertIsNone(ambiguo["numero_rg"])
        self.assertIsNone(ambiguo["data_atualizacao"])
        self.assertTrue(ambiguo["flag_rg_ambiguo"])
        self.assertTrue(ambiguo["possui_certidao_nascimento"])
        sem_evidencia = resultado[1]["documentos_cadunico"]
        self.assertIsNone(sem_evidencia["possui_rg"])
        self.assertIsNone(sem_evidencia["possui_certidao_nascimento"])

    def test_familias_preservam_saidas_servicos_encerrados_e_responsavel_sem_membro(
        self,
    ):
        membro = dict(
            id_paciente=1,
            id_familia=10,
            id_membro=7,
            data_entrada="2025-01-01",
            data_saida="2026-01-15",
        )
        fixtures = {
            "raw_membros_familia": fixture_rows(
                {
                    "id_paciente": "int64",
                    "id_familia": "int64",
                    "id_membro": "int64",
                    "id_familia_nova": "int64",
                    "id_motivo_saida": "int64",
                    "data_entrada": "date",
                    "data_saida": "date",
                    "parentesco_responsavel": "string",
                },
                [membro, membro],
            ),
            "raw_familias": fixture_rows(
                {
                    "id_familia": "int64",
                    "id_usuario_responsavel": "int64",
                    "flag_ativo": "string",
                    "data_ultima_modificacao": "datetime",
                },
                [dict(id_familia=10, id_usuario_responsavel=2, flag_ativo="S")],
            ),
            "raw_familias_servicos_assistenciais": fixture_rows(
                {
                    "id_familia": "int64",
                    "id_servico_assistencial": "int64",
                    "id_unidade": "int64",
                    "id_login_cadastro": "int64",
                    "data_cadastro": "datetime",
                    "data_cancelamento": "datetime",
                },
                [
                    dict(
                        id_familia=10,
                        id_servico_assistencial=1,
                        id_unidade=20,
                        data_cadastro="2025-01-01",
                        data_cancelamento="2026-01-20",
                    )
                ],
            ),
        }
        resultado = self.consultar_modelo(
            "models/intermediate/planilha_unificada/familias/int_pu_familias.sql",
            fixtures,
        )
        self.assertEqual([r["id_usuario"] for r in resultado], [1, 2])
        familia = resultado[0]["familias"][0]
        self.assertEqual(len(familia["vinculos"]), 1)
        self.assertEqual(familia["vinculos"][0]["data_saida"], "2026-01-15")
        self.assertTrue(
            familia["servicos"][0]["data_cancelamento"].startswith("2026-01-20")
        )
        responsavel = resultado[1]["familias"][0]
        self.assertTrue(responsavel["flag_responsavel_atual"])
        self.assertEqual(responsavel["vinculos"], [])

    def test_formularios_preservam_historico_nao_cancelado_campos_observacoes(self):
        schema = {
            "id_usuario": "int64",
            "id_evolucao_modulo": "string",
            "id_evolucao": "int64",
            "origem_modulo": "string",
            "id_familia": "int64",
            "id_unidade": "int64",
            "id_profissional": "int64",
            "codigo_abrangencia": "int64",
            "id_atividade": "int64",
            "data_evolucao": "datetime",
            "data_cancelamento": "datetime",
            "tipo_evolucao": "string",
            "descricao_evolucao": "string",
        }
        registro = dict(
            id_usuario=1,
            id_evolucao_modulo="usuario:7",
            id_evolucao=7,
            origem_modulo="usuario",
            id_unidade=10,
            codigo_abrangencia=29,
            data_evolucao="2026-01-02",
            descricao_evolucao="<h3>Centro POP - Atendimento Social</h3><p>Demanda: <b>Documento</b></p>"
            "<h3>Observações</h3><p>Observação do formulário</p>",
        )
        outro = dict(
            registro,
            id_evolucao_modulo="familia:7",
            origem_modulo="familia",
            data_evolucao="2026-02-02",
            data_cancelamento=None,
            id_unidade=20,
        )
        resultado = self.consultar_modelo(
            "models/intermediate/planilha_unificada/formularios/int_pu_formularios.sql",
            {
                "int_pu_evolucoes": fixture_rows(schema, [registro, registro, outro]),
            },
        )
        forms = resultado[0]["formularios"]
        self.assertEqual(len(forms), 2)
        self.assertEqual(forms[0]["campos"], [dict(label="Demanda", valor="Documento")])
        self.assertIn("Observação do formulário", forms[0]["observacoes"])
        self.assertEqual(forms[1]["id_evolucao_modulo"], "familia:7")
        self.assertIsNone(forms[1]["data_cancelamento"])

    def test_entidades_html_preservam_null_texto_e_entidades_desconhecidas(self):
        casos = [
            (None, None),
            ("", ""),
            ("Não, ação e informação", "Não, ação e informação"),
            ("N&atilde;o", "Não"),
            ("A&ccedil;&atilde;o &Eacute; &Uacute;til", "Ação É Útil"),
            (
                "&quot;sim&quot; &apos;não&apos; &ordm; &ordf; &deg;",
                "\"sim\" 'não' º ª °",
            ),
            ("N&#227;o N&#xE3;o N&#X00E3;o", "Não Não Não"),
            ("&#39; &#x1F600;", "' 😀"),
            ("A&nbsp;B &amp; C &lt;b&gt;literal&lt;/b&gt;", "A B & C <b>literal</b>"),
            (
                "&desconhecida; &#0; &#55296; &#1114112; &#999999999999999999999999; &#xZZ;",
                "&desconhecida; &#0; &#55296; &#1114112; &#999999999999999999999999; &#xZZ;",
            ),
            ("&amp;atilde;", "&atilde;"),
            (
                "Antes\nDepois & sem ponto e vírgula",
                "Antes\nDepois & sem ponto e vírgula",
            ),
        ]
        env = Environment(undefined=StrictUndefined, autoescape=False)  # noqa: S701
        macro = env.from_string(
            (QUERIES / "macros/decodificar_entidades_html.sql").read_text()
        ).module.decodificar_entidades_html
        entrada = fixture_rows(
            {"id_usuario": "int64", "texto": "string"},
            [dict(id_usuario=i, texto=texto) for i, (texto, _) in enumerate(casos)],
        )
        expressao = macro("texto")
        consulta = f"select id_usuario, {expressao} as normalizado from ({entrada}) order by id_usuario"  # noqa: S608
        obtido = [r.normalizado for r in self.client.query(consulta).result(timeout=60)]
        self.assertEqual(obtido, [esperado for _, esperado in casos])

    def test_formularios_normalizam_textos_antes_de_deduplicar(self):
        schema = {
            "id_usuario": "int64",
            "id_evolucao_modulo": "string",
            "id_evolucao": "int64",
            "origem_modulo": "string",
            "id_familia": "int64",
            "id_unidade": "int64",
            "id_profissional": "int64",
            "codigo_abrangencia": "int64",
            "id_atividade": "int64",
            "data_evolucao": "datetime",
            "data_cancelamento": "datetime",
            "tipo_evolucao": "string",
            "descricao_evolucao": "string",
        }
        registro = dict(
            id_usuario=1,
            id_evolucao_modulo="usuario:7",
            id_evolucao=7,
            origem_modulo="usuario",
            id_unidade=10,
            data_evolucao="2026-01-02",
            descricao_evolucao="<h3>Situa&ccedil;&atilde;o</h3>"
            "<p>Informa&ccedil;&atilde;o: <b>N&atilde;o</b></p>"
            "<p>Informação: <b>Não</b></p>"
            "<p>Texto literal: <b>&lt;exemplo&gt;</b></p>"
            "<h3>Observações</h3><p>A&ccedil;&atilde;o registrada</p>",
        )
        resultado = self.consultar_modelo(
            "models/intermediate/planilha_unificada/formularios/int_pu_formularios.sql",
            {"int_pu_evolucoes": fixture_rows(schema, [registro])},
        )
        formulario = resultado[0]["formularios"][0]
        self.assertEqual(formulario["titulo_formulario"], "Situação")
        self.assertEqual(
            formulario["campos"],
            [
                dict(label="Informação", valor="Não"),
                dict(label="Texto literal", valor="<exemplo>"),
            ],
        )
        self.assertEqual(formulario["observacoes"], "<p>Ação registrada</p>")

    def test_evolucoes_familiares_usam_apenas_o_paciente_explicito(self):
        schema = {
            "id_usuario": "int64",
            "id_paciente_familia": "int64",
            "id_evolucao": "int64",
            "origem_modulo": "string",
            "id_familia": "int64",
            "id_unidade": "int64",
            "id_profissional": "int64",
            "codigo_abrangencia": "int64",
            "id_atividade": "int64",
            "data_evolucao": "datetime",
            "data_cancelamento": "datetime",
            "tipo_evolucao": "string",
            "descricao_evolucao": "string",
        }
        resultado = self.consultar_modelo(
            "models/intermediate/planilha_unificada/formularios/int_pu_evolucoes.sql",
            {
                "int_evolucoes": fixture_rows(
                    schema,
                    [
                        dict(
                            id_familia=99,
                            id_paciente_familia=1,
                            id_evolucao=7,
                            origem_modulo="familia",
                        ),
                        dict(id_familia=1, id_evolucao=8, origem_modulo="familia"),
                        dict(id_usuario=2, id_evolucao=7, origem_modulo="usuario"),
                        dict(
                            id_usuario=2,
                            id_evolucao=8,
                            origem_modulo="usuario",
                            data_cancelamento="2026-10-23 10:00:00",
                        ),
                        dict(
                            id_usuario=5,
                            id_evolucao=9,
                            origem_modulo="usuario",
                            data_cancelamento="2026-10-23 10:00:00",
                        ),
                        dict(
                            id_usuario=3,
                            id_evolucao=9,
                            origem_modulo="administrativa",
                            id_profissional=77,
                        ),
                        dict(
                            id_usuario=4,
                            id_evolucao=10,
                            origem_modulo="usuario",
                            id_profissional=77,
                        ),
                    ],
                ),
                "dim_profissionais": "select 77 id_profissional, 'Nome do profissional' nome",
                "raw_operadores": "select 77 id_login, 'Nome do operador' nome_operador",
            },
        )
        self.assertEqual([r["id_usuario"] for r in resultado], [1, 2, 3, 4])
        self.assertEqual(
            [r["id_evolucao_modulo"] for r in resultado],
            ["familia:7", "usuario:7", "administrativa:9", "usuario:10"],
        )
        self.assertEqual(resultado[2]["nome_profissional"], "Nome do operador")
        self.assertEqual(resultado[3]["nome_profissional"], "Nome do profissional")

    def fixtures_saida_enxuta(self):
        strings = "nome nome_social filiacao_mae nacionalidade condicao_estrangeira pais_origem cpf sexo raca_cor genero bairro prontuario flag_cadunico grau_dependencia nome_apoiador numero_processo_decisao_apoiada atvd_remunerada flag_decisao_apoiada flag_saude_mental_comprometida motivo_acolhimento orientacao_sexual vinculo_trabalhista flag_trabalha profissao flag_frequenta_escola escolaridade_indice flag_recebe_beneficio flag_curatela tipo_curatela flag_deficiencia flag_situacao_rua origem_demanda flag_possui_violacao_direito id_usuario_sk".split()
        schema = {name: "string" for name in strings}
        schema.update(
            id_usuario="int64",
            id_unidade_referencia="int64",
            pontuacao="int64",
            data_nascimento="date",
            renda_ativa="numeric",
            id_login_cadastro="int64",
        )
        usuarios = fixture_rows(
            schema,
            [
                dict(
                    id_usuario=1,
                    nome="Pessoa A",
                    id_usuario_sk="sk1",
                    id_unidade_referencia=10,
                ),
                dict(id_usuario=2, nome="Pessoa B", id_usuario_sk="sk2"),
            ],
        )
        fixtures = {
            "dim_usuarios": "select u.*, array<struct<codigo string, descricao string, origem string>>[] violacoes, array<struct<codigo string, descricao string>>[] deficiencia, array<struct<codigo string, descricao string>>[] beneficio, if(id_usuario=1,[struct(2 as id_projeto_social,10 as id_unidade,'S' as indicador_ativo,cast(null as datetime) as data_cadastro,cast(null as datetime) as data_cancelamento)],[]) projetos_sociais from ("  # noqa: S608
            + usuarios
            + ") u",
            "int_pu_evolucoes": "select 1 id_usuario, 'usuario:7' id_evolucao_modulo",
            "int_pu_cadastro": "select 1 id_usuario, struct('RJ' as uf_nascimento,cast(null as string) as serie_escolar,cast(null as date) as data_cadunico_informada) cadastro_complementar",
            "int_pu_documentos_cadunico": "select 1 id_usuario, struct('123' as numero_rg,'111' as cpf_normalizado,20 as id_familia_cadunico,cast(null as string) as nis,cast(null as string) as numero_certidao_nascimento,cast(null as bool) as possui_rg,cast(null as bool) as possui_certidao_nascimento,cast(null as date) as data_atualizacao,cast(null as date) as data_limite_cadastro_atual,false as flag_familia_ambigua,false as flag_nis_ambiguo,false as flag_rg_ambiguo,false as flag_certidao_ambigua) documentos_cadunico",
            "int_pu_familias": """select 1 id_usuario, [struct(10 as id_familia,2 as id_usuario_responsavel,'Responsável' as nome_responsavel,true as flag_responsavel_atual,cast(null as string) as flag_ativo,cast(null as date) as data_ultima_modificacao,[struct(7 as id_membro,20 as id_familia_nova,1 as id_motivo_saida,'Mudança' as motivo_saida,cast(null as date) as data_entrada,cast(null as date) as data_saida,cast(null as string) as parentesco_responsavel)] as vinculos,[struct(1 as id_servico_assistencial,'PAIF' as nome_servico,10 as id_unidade,'Unidade A' as nome_unidade,100 as id_login_cadastro,cast(null as datetime) as data_cadastro,cast(null as datetime) as data_cancelamento)] as servicos)] familias""",
            "int_pu_atendimentos": """select 1 id_usuario, [struct('7u' as id_atendimento_modulo,7 as id_atendimento,1 as id_tipo_atendimento,10 as id_unidade,'Unidade A' as nome_unidade,'Atendimento' as tipo_atendimento_descricao,'usuario' as origem_modulo,cast(null as date) as data_atendimento,cast(null as string) as hora_atendimento,cast(null as string) as flag_cancelado,[struct(100 as id_profissional,'Alex' as nome_profissional,['2516'] as codigos_cbo,['Assistente Social'] as descricoes_cbo,'2516' as cbo_principal_codigo,'Assistente Social' as cbo_principal_descricao),struct(101,'Alex',['2516'],['Assistente Social'],'2516','Assistente Social')] as profissionais)] atendimentos""",
            "int_pu_formularios": """select 1 id_usuario,[struct('usuario:7' as id_evolucao_modulo,7 as id_evolucao,100 as id_profissional,29 as codigo_abrangencia,20 as id_atividade,10 as id_unidade,10 as id_familia,'Alex' as nome_profissional,'Oficina' as nome_atividade,'usuario' as origem_modulo,cast(null as datetime) as data_evolucao,cast(null as datetime) as data_cancelamento,cast(null as string) as tipo_evolucao,cast(null as string) as titulo_formulario,cast(null as string) as observacoes,array<struct<label string,valor string>>[] as campos)] formularios""",
        }

        fixtures["int_pu_evolucoes"] = (
            "select p.id_usuario, f.* except(titulo_formulario, observacoes, campos), "  # noqa: S608 - SQL de fixtures locais, valores escapados por sql_literal.
            "'<p>Relato registrado</p>' as descricao_evolucao, 'Unidade A' as nome_unidade from ("
            + fixtures["int_pu_formularios"]
            + ") p, unnest(p.formularios) f"
        )
        return fixtures

    def consultar_saida(self, fixtures=None):
        return self.consultar_modelo(
            "models/marts/planilha_unificada/planilha_unificada.sql",
            fixtures if fixtures is not None else self.fixtures_saida_enxuta(),
        )

    def test_cinco_blocos_preservam_usuario_sem_eventos_e_ids_permitidos(self):
        resultado = self.consultar_saida()
        self.assertEqual(len(resultado), 2)
        self.assertEqual(
            set(resultado[0]),
            {"usuario", "familias", "projetos_sociais", "atendimentos", "evolucoes"},
        )
        self.assertEqual(resultado[1]["usuario"]["nome"], "Pessoa B")
        for bloco in ["atendimentos", "evolucoes"]:
            self.assertEqual(resultado[0][bloco]["quantidade"], 1)
            self.assertEqual(
                resultado[1][bloco],
                {
                    "quantidade": 0,
                    "quantidade_por_servico": dict.fromkeys(
                        ["cras", "creas", "centro_pop", "outros"], 0
                    ),
                    "registros": [],
                },
            )
        self.assertEqual(resultado[1]["familias"], [])
        self.assertEqual(resultado[1]["projetos_sociais"], [])
        self.assertTrue(
            all(
                v == "" for v in resultado[1]["usuario"]["documentos_cadunico"].values()
            )
        )
        self.assertEqual(
            resultado[0]["usuario"]["nome_unidade_referencia"], "Unidade A"
        )
        self.assertEqual(
            resultado[0]["evolucoes"]["registros"][0]["descricao_evolucao"],
            "Relato registrado",
        )
        self.assertEqual(
            len(resultado[0]["atendimentos"]["registros"][0]["profissionais"]), 2
        )
        permitidos = {
            "id_usuario",
            "id_familia",
            "id_atendimento_modulo",
            "id_evolucao_modulo",
        }

        def conferir(valor):
            if isinstance(valor, dict):
                for nome, item in valor.items():
                    if nome.startswith("id_"):
                        self.assertIn(nome, permitidos)
                    self.assertNotIn(
                        nome,
                        {
                            "codigo",
                            "codigos_cbo",
                            "cbo_principal_codigo",
                            "codigo_abrangencia",
                        },
                    )
                    if nome == "quantidade" or nome in {
                        "cras",
                        "creas",
                        "centro_pop",
                        "outros",
                    }:
                        self.assertIsInstance(item, int)
                    else:
                        conferir(item)
            elif isinstance(valor, list):
                for item in valor:
                    conferir(item)
            else:
                self.assertIsInstance(valor, str)

        conferir(resultado)

    def test_textos_nulos_e_valores_informados_no_usuario_e_formularios(self):
        fixtures = self.fixtures_saida_enxuta()
        fixtures["dim_usuarios"] = (
            fixtures["dim_usuarios"]
            .replace("cast(null as string) as nome_social", "'null' as nome_social")
            .replace("cast(null as string) as cpf", "'000123' as cpf")
            .replace(
                "cast(null as string) as nome_apoiador",
                "' Nome com espaços ' as nome_apoiador",
            )
        )
        fixtures["int_pu_formularios"] = fixtures["int_pu_formularios"].replace(
            "array<struct<label string,valor string>>[] as campos",
            "[struct(cast(null as string) as label,cast(null as string) as valor),struct('Resposta','Não'),struct('Número','0')] as campos",
        )
        linha = self.consultar_saida(fixtures)[0]
        u = linha["usuario"]
        self.assertEqual(u["nome_social"], "null")
        self.assertEqual(u["cpf"], "000123")
        self.assertEqual(u["nome_apoiador"], " Nome com espaços ")
        self.assertEqual(u["cadastro_complementar"]["serie_escolar"], "")
        self.assertEqual(u["documentos_cadunico"]["nis"], "")
        self.assertEqual(u["documentos_cadunico"]["flag_nis_ambiguo"], "false")
        self.assertEqual(
            linha["evolucoes"]["registros"][0]["formularios"][0]["campos"],
            [
                {"label": "", "valor": ""},
                {"label": "Resposta", "valor": "Não"},
                {"label": "Número", "valor": "0"},
            ],
        )

    def test_representacao_de_datas_numeros_e_booleanos_no_usuario(self):
        fixtures = self.fixtures_saida_enxuta()
        fixtures["dim_usuarios"] = (
            fixtures["dim_usuarios"]
            .replace(
                "cast(null as date) as data_nascimento",
                "date '2000-01-02' as data_nascimento",
            )
            .replace("cast(null as int64) as pontuacao", "42 as pontuacao")
            .replace(
                "cast(null as numeric) as renda_ativa",
                "numeric '123.45' as renda_ativa",
            )
        )
        u = self.consultar_saida(fixtures)[0]["usuario"]
        self.assertEqual(u["data_nascimento"], "2000-01-02")
        self.assertEqual(u["pontuacao"], "42")
        self.assertEqual(u["renda_ativa"], "123.45")
        self.assertEqual(u["documentos_cadunico"]["flag_nis_ambiguo"], "false")

    def test_normalizacao_de_horas_na_saida_final(self):
        casos = [
            ("0900", "09:00"),
            ("1430", "14:30"),
            ("0000", "00:00"),
            ("2359", "23:59"),
            (" 0900 ", "09:00"),
            ("9:05", "09:05"),
            ("09:05", "09:05"),
            ("", ""),
            (None, ""),
            ("2460", ""),
            ("1260", ""),
            ("abc", ""),
            ("900", ""),
        ]
        fixtures = self.fixtures_saida_enxuta()
        base = fixtures["int_pu_atendimentos"]
        fixtures["int_pu_atendimentos"] = (
            "select 1 id_usuario, array(select as struct a.* replace(h as hora_atendimento) from ("  # noqa: S608 - SQL de fixtures locais, valores escapados por sql_literal.
            + base
            + ") b, unnest(b.atendimentos) a, unnest(["
            + ",".join("struct(" + sql_literal(h) + " as h)" for h, _ in casos)
            + "]) v with offset as posicao order by posicao) atendimentos"
        )
        bloco = self.consultar_saida(fixtures)[0]["atendimentos"]
        self.assertCountEqual(
            [a["hora_atendimento"] for a in bloco["registros"]], [v for _, v in casos]
        )
        self.assertEqual(bloco["quantidade"], len(casos))

    def test_evolucoes_completas_formularios_associados_e_modulos_distintos(self):
        fixtures = self.fixtures_saida_enxuta()
        base = fixtures["int_pu_evolucoes"]
        fixtures["int_pu_evolucoes"] = (
            base  # noqa: S608 - SQL de fixtures locais, valores escapados por sql_literal.
            + " union all select e.* replace('familia:7' as id_evolucao_modulo, 'familia' as origem_modulo) from ("
            + base
            + ") e"
            + " union all select e.* replace('grupo:8' as id_evolucao_modulo, 'grupo' as origem_modulo) from ("
            + base
            + ") e"
            + " union all select e.* replace(2 as id_usuario, 'usuario:9' as id_evolucao_modulo) from ("
            + base
            + ") e"
        )
        forms = fixtures["int_pu_formularios"]
        fixtures["int_pu_formularios"] = (
            "select e.* replace(array_concat(e.formularios, array(select as struct f.* replace('Outro' as titulo_formulario) "  # noqa: S608 - SQL de fixtures locais, valores escapados por sql_literal.
            "from unnest(e.formularios) f)) as formularios) from (" + forms + ") e"
        )
        resultado = self.consultar_saida(fixtures)
        self.assertEqual(resultado[0]["evolucoes"]["quantidade"], 3)
        registros = {
            v["id_evolucao_modulo"]: v for v in resultado[0]["evolucoes"]["registros"]
        }
        self.assertEqual(len(registros["usuario:7"]["formularios"]), 2)
        self.assertEqual(registros["familia:7"]["formularios"], [])
        self.assertEqual(registros["grupo:8"]["formularios"], [])
        self.assertEqual(resultado[1]["evolucoes"]["quantidade"], 1)
        self.assertEqual(resultado[1]["evolucoes"]["registros"][0]["formularios"], [])

    def test_nomes_de_unidade_homonimos_nao_colapsam_registros(self):
        fixtures = self.fixtures_saida_enxuta()
        base = fixtures["int_pu_atendimentos"]
        fixtures["int_pu_atendimentos"] = (
            "select b.id_usuario,array_concat(b.atendimentos,array(select as struct a.* replace(20 as id_unidade,'7f' as id_atendimento_modulo) "  # noqa: S608 - SQL de fixtures locais, valores escapados por sql_literal.
            "from unnest(b.atendimentos) a)) atendimentos from (" + base + ") b"
        )
        linha = self.consultar_saida(fixtures)[0]
        self.assertEqual(linha["atendimentos"]["quantidade"], 2)
        self.assertEqual(
            [a["nome_unidade"] for a in linha["atendimentos"]["registros"]],
            ["Unidade A", "Unidade A"],
        )
        self.assertEqual(linha["projetos_sociais"][0]["nome_unidade"], "Unidade A")
        self.assertEqual(
            linha["familias"][0]["servicos"][0]["nome_unidade"], "Unidade A"
        )
        self.assertEqual(
            linha["evolucoes"]["registros"][0]["nome_unidade"], "Unidade A"
        )

    def test_unidade_ausente_e_servico_sem_traducao_nao_excluem_registros(self):
        fixtures = self.fixtures_saida_enxuta()
        fixtures["int_pu_atendimentos"] = (
            fixtures["int_pu_atendimentos"]
            .replace("10 as id_unidade", "81 as id_unidade")
            .replace(
                "'Unidade A' as nome_unidade", "cast(null as string) as nome_unidade"
            )
        )
        fixtures["int_pu_familias"] = fixtures["int_pu_familias"].replace(
            "1 as id_servico_assistencial,'PAIF' as nome_servico",
            "2 as id_servico_assistencial,cast(null as string) as nome_servico",
        )
        linha = self.consultar_saida(fixtures)[0]
        self.assertEqual(linha["atendimentos"]["quantidade"], 1)
        self.assertEqual(linha["atendimentos"]["registros"][0]["nome_unidade"], "")
        self.assertEqual(linha["familias"][0]["servicos"][0]["nome_servico"], "")

    def test_descricao_html_legivel_preserva_texto_escapado_e_quebras(self):
        fixtures = self.fixtures_saida_enxuta()
        base = fixtures["int_pu_evolucoes"]
        texto = "<style>.x{color:red}</style><!--oculto--><p>A<br>B</p><p>N&atilde;o &lt;exemplo&gt;</p><script>alert(1)</script>"
        fixtures["int_pu_evolucoes"] = (
            "select e.* replace("  # noqa: S608 - SQL de fixtures locais, valores escapados por sql_literal.
            + sql_literal(texto)
            + " as descricao_evolucao) from ("
            + base
            + ") e"
        )
        linha = self.consultar_saida(fixtures)[0]
        self.assertEqual(
            linha["evolucoes"]["registros"][0]["descricao_evolucao"],
            "A\nB\nNão <exemplo>",
        )

    def test_descricao_ausente_e_texto_sem_html(self):
        fixtures = self.fixtures_saida_enxuta()
        base = fixtures["int_pu_evolucoes"]
        fixtures["int_pu_evolucoes"] = (
            "select e.* replace(cast(null as string) as descricao_evolucao) from ("  # noqa: S608 - SQL de fixtures locais, valores escapados por sql_literal.
            + base
            + ") e "
            "union all select e.* replace('usuario:8' as id_evolucao_modulo,'Linha 1\\nLinha 2 &amp; fim' as descricao_evolucao) from ("
            + base
            + ") e"
        )
        registros = self.consultar_saida(fixtures)[0]["evolucoes"]["registros"]
        self.assertEqual(registros[0]["descricao_evolucao"], "")
        self.assertEqual(registros[1]["descricao_evolucao"], "Linha 1\nLinha 2 & fim")

    def fixtures_multiplos_servicos(self):
        fixtures = self.fixtures_saida_enxuta()
        attended = fixtures["int_pu_atendimentos"]
        fixtures["int_pu_atendimentos"] = (
            "select p.id_usuario,array(select as struct a.* replace(concat(cast(posicao as string),'u') as id_atendimento_modulo), s as tipo_servico "
            "from unnest(p.atendimentos) a, unnest(['cras','creas','centro_pop','outros']) s with offset as posicao order by posicao) atendimentos from ("
            + attended
            + ") p"
        )
        evo = fixtures["int_pu_evolucoes"]
        fixtures["int_pu_evolucoes"] = (
            "select e.* replace('<h3>Acompanhamento</h3><p>Demanda: <b>N&atilde;o</b></p>' as descricao_evolucao), 'centro_pop' tipo_servico from ("
            + evo
            + ") e union all select e.* replace(2 as id_usuario,'usuario:9' as id_evolucao_modulo), 'creas' tipo_servico from ("
            + evo
            + ") e"
        )
        return fixtures

    def test_profissional_sem_cbo_tem_listas_vazias_antes_de_materializar(self):
        fixtures = self.fixtures_saida_enxuta()
        fixtures["fct_atendimentos"] = (
            "select p.id_usuario,a.* except(profissionais),100 id_profissional,'sem_cadastro' id_profissional_sk from ("
            + fixtures["int_pu_atendimentos"]
            + ") p, unnest(p.atendimentos) a"
        )
        fixtures["dim_profissionais"] = (
            "select cast(null as string) id_profissional_sk,cast(null as string) nome,cast(null as array<string>) codigos_cbo,"
            "cast(null as array<string>) descricoes_cbo,cast(null as string) cbo_principal_codigo,cast(null as string) cbo_principal_descricao from unnest([1]) where false"
        )
        rows = self.consultar_modelo(
            "models/intermediate/planilha_unificada/atendimentos/int_pu_atendimentos.sql",
            fixtures,
        )
        professional = rows[0]["atendimentos"][0]["profissionais"][0]
        self.assertEqual(professional["codigos_cbo"], [])
        self.assertEqual(professional["descricoes_cbo"], [])
        self.assertEqual(professional["id_profissional"], 100)

    def test_dimensoes_separam_servicos_e_preservam_usuario_so_com_evolucao(self):
        fixtures = self.fixtures_multiplos_servicos()
        for service, model in [
            ("cras", "dim_cras"),
            ("creas", "dim_creas"),
            ("centro_pop", "dim_centro_pop"),
            ("outros", "dim_outros_equipamentos"),
        ]:
            with self.subTest(service=service):
                rows = self.consultar_modelo(
                    "models/intermediate/planilha_unificada/dimensoes/"
                    + model
                    + ".sql",
                    fixtures,
                )
                self.assertEqual(len(rows), 2 if service == "creas" else 1)
                self.assertEqual(len(rows[0]["atendimentos"]), 1)
                self.assertEqual(rows[0]["atendimentos"][0]["tipo_servico"], service)
                if service == "centro_pop":
                    self.assertEqual(
                        rows[0]["evolucoes"][0]["formularios"][0]["campos"],
                        [{"label": "Demanda", "valor": "Não"}],
                    )
                elif service == "creas":
                    self.assertEqual(rows[1]["atendimentos"], [])
                    self.assertEqual(len(rows[1]["evolucoes"]), 1)
                else:
                    self.assertEqual(rows[0]["evolucoes"], [])

    def test_contagens_total_e_por_servico_com_usuario_em_varias_dimensoes(self):
        rows = self.consultar_saida(self.fixtures_multiplos_servicos())
        self.assertEqual(rows[0]["atendimentos"]["quantidade"], 4)
        self.assertEqual(
            rows[0]["atendimentos"]["quantidade_por_servico"],
            {"cras": 1, "creas": 1, "centro_pop": 1, "outros": 1},
        )
        self.assertEqual(
            rows[0]["evolucoes"]["quantidade_por_servico"],
            {"cras": 0, "creas": 0, "centro_pop": 1, "outros": 0},
        )
        self.assertEqual(
            rows[1]["evolucoes"]["quantidade_por_servico"],
            {"cras": 0, "creas": 1, "centro_pop": 0, "outros": 0},
        )
        self.assertEqual(rows[1]["atendimentos"]["quantidade"], 0)


if __name__ == "__main__":
    unittest.main()
