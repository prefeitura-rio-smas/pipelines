"""Synthetic SQL tests for dated answers; no live materialization."""

# ruff: noqa: S608
import os
import unittest

import test_modelos as base
from test_modelos import cpop_fields, fixture_rows, sql_literal


@unittest.skipUnless(
    os.getenv("RUN_BIGQUERY_TESTS") == "1", "Consultas sintéticas BigQuery opcionais"
)
class CentroPopScopeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from google.cloud import bigquery

        cls.runner = base.PlanilhaUnificadaTests()
        cls.runner.client = bigquery.Client(
            project=os.getenv("BIGQUERY_TEST_PROJECT", "rj-smas-dev")
        )

    def fixtures(self, contradictory=False):
        def form(key, title, when, fields, unit=10, cancel=False):
            schema = dict(
                id_evolucao_modulo="STRING",
                id_unidade="INT64",
                data_evolucao="DATETIME",
                data_cancelamento="DATETIME",
                titulo_formulario="STRING",
                observacoes="STRING",
            )
            data = dict(
                id_evolucao_modulo="usuario:" + str(key),
                id_unidade=unit,
                data_evolucao=when,
                data_cancelamento=when if cancel else None,
                titulo_formulario=title,
                observacoes="<p>Não informado pelo usuário</p>",
            )
            fields_sql = (
                "["
                + ",".join(
                    f"struct({sql_literal(label)} as label,{sql_literal(value)} as valor)"
                    for label, value in fields
                )
                + "]"
            )
            return (
                "select 1 id_usuario,array(select as struct f.*,"
                + "cast("
                + fields_sql
                + " as array<struct<label string,valor string>>) as campos from ("
                + fixture_rows(schema, [data])
                + ") f) formularios"
            )

        social = [
            ("Possui referências familiares?", "S"),
            ("Território da referência familiar:", "Rio"),
            ("Existe possibilidade de reinserção familiar?", "N"),
            ("Demanda inicial:", "Documentos"),
            ("Identificação das demandas apresentadas pelo usuário:", "Saúde"),
            ("Encaminhamentos: saúde", "UBS"),
            ("Encaminhamentos: documentos", "Cartório"),
        ]
        if contradictory:
            social.append(("Possui referências familiares?", "Não"))
        forms = [
            form(
                1,
                "Centro POP - Plano de Atendimento Individual (PAI)",
                "2026-01-15 14:00:00",
                [("Data do atendimento:", "10/01/2026 11:35")],
            ),
            form(
                2,
                "Centro POP - Plano de Atendimento Individual (PAI)",
                "2026-02-03 09:00:00",
                [("Data do atendimento:", "2026-02-01T15:00:00")],
            ),
            form(3, "Centro POP - Atendimento Social", "2026-01-20 10:00:00", social),
            form(
                4,
                "Centro POP - Atendimento Social",
                "2026-01-21 10:00:00",
                [("Território da referência familiar", "Outra unidade")],
                unit=20,
            ),
            form(
                5,
                "Situação de Saúde",
                "2026-01-01 10:00:00",
                [
                    ("Faz uso de substâncias psicoativas?", "S"),
                    ("Faz uso de substâncias psicoativas?", "Sim"),
                    ("Situação:", "Asma"),
                    ("Local onde faz tratamento:", "N"),
                ],
                unit=30,
            ),
            form(
                6,
                "Situação de Saúde",
                "2026-03-01 10:00:00",
                [("Situação:", "N")],
                unit=30,
            ),
            form(
                7,
                "Centro POP - Desligamento PAI",
                "2026-04-05 10:00:00",
                [
                    ("Data do desligamento:", "01/04/2026 08:00"),
                    ("Motivo do desligamento:", "Reinserção"),
                    ("Outros:", "Complemento"),
                ],
            ),
            form(
                8,
                "Centro POP - Atendimento Social",
                "2026-02-03 10:00:00",
                [("Território da referência familiar", "Cancelado")],
                cancel=True,
            ),
            form(
                9,
                "Centro POP - Plano de Atendimento Individual (PAI)",
                "2026-04-10 10:00:00",
                [],
            ),
        ]
        return {
            "dim_usuarios": "select 1 id_usuario,'Pessoa A' nome union all select 2,'Pessoa B'",
            "dim_unidades": "select 10 id_unidade,'Centro POP A' nome_unidade,'Centro POP' tipo_unidade union all select 20,'Centro POP B','Centro POP' union all select 30,'CRAS A','CRAS'",
            "int_pu_atendimentos": "select id_usuario,[struct('centro_pop' as tipo_servico)] atendimentos from unnest([1,2]) id_usuario",
            "int_pu_formularios": " union all ".join(forms),
            "raw_presencas_usuarios": fixture_rows(
                dict(
                    id_usuario="INT64",
                    id_unidade="INT64",
                    id_atividade="INT64",
                    data_presenca="DATE",
                    hora_presenca="STRING",
                ),
                [
                    dict(
                        id_usuario=1,
                        id_unidade=10,
                        id_atividade=100,
                        data_presenca="2026-02-05",
                        hora_presenca="9:05",
                    ),
                    dict(
                        id_usuario=1,
                        id_unidade=10,
                        id_atividade=100,
                        data_presenca="2026-02-05",
                        hora_presenca="9:05",
                    ),
                    dict(
                        id_usuario=1,
                        id_unidade=10,
                        id_atividade=100,
                        data_presenca="2026-02-06",
                        hora_presenca="9:05",
                    ),
                    dict(
                        id_usuario=1,
                        id_unidade=10,
                        id_atividade=101,
                        data_presenca="2026-02-05",
                        hora_presenca="9:05",
                    ),
                    dict(
                        id_usuario=1,
                        id_unidade=30,
                        id_atividade=102,
                        data_presenca="2026-02-05",
                        hora_presenca="9:05",
                    ),
                ],
            ),
            "raw_atividades_grupo": "select id_atividade,'Oficina' nome_atividade from unnest([100,101,102]) id_atividade",
            "raw_evolucoes_questionario": "select 1 id_prontuario,1 id_evolucao,0 id_modulo,3 id_template,datetime '2026-01-02 10:00:00' data_evolucao",
            "raw_evolucoes_questionario_lista": "select 1 id_evolucao,0 id_modulo,3 id_template,18 id_questao,datetime '2026-01-03 10:00:00' data_resposta,'Motivo principal' resposta union all select 1,1,3,18,datetime '2026-01-03 10:00:00','Módulo errado'",
        }

    def read(self, fixtures=None):
        return self.runner.consultar_modelo(
            "models/intermediate/planilha_unificada/dimensoes/int_pu_registros_centro_pop.sql",
            fixtures or self.fixtures(),
        )

    def test_dated_answers_no_monthly_collapse_or_artificial_unit(self):
        rows = self.read()
        self.assertEqual({r["id_usuario"] for r in rows}, {1})
        self.assertEqual(len({r["id_registro"] for r in rows}), len(rows))
        pai = sorted(
            [r for r in rows if r["tipo_registro"] == "pai"],
            key=lambda r: r["data_registro"],
        )
        self.assertEqual(
            [r["data_inclusao_pai"] for r in pai], ["2026-01-10", "2026-02-01", None]
        )
        self.assertEqual(pai[-1]["data_registro"], "2026-04-10T10:00:00")
        saude = sorted(
            [r for r in rows if r["tipo_registro"] == "saude"],
            key=lambda r: r["data_registro"],
        )
        self.assertEqual([r["possui_problema_saude"] for r in saude], ["Sim", "Não"])
        self.assertTrue(
            all(r["abrangencia"] == "pessoa" and r["id_unidade"] == 30 for r in saude)
        )
        self.assertEqual(saude[0]["realiza_acompanhamento_saude"], "Não")
        anterior = [
            r for r in rows if r["data_registro"] and r["data_registro"] < "2026-03-01"
        ]
        self.assertEqual(len([r for r in anterior if r["tipo_registro"] == "pai"]), 2)
        principal = [r for r in rows if r["tipo_registro"] == "situacao_rua"]
        self.assertEqual(len(principal), 1)
        self.assertIsNone(principal[0]["id_unidade"])
        self.assertEqual(
            principal[0]["motivo_principal_permanencia_rua"], "Motivo principal"
        )
        self.assertTrue(all(r["id_evolucao_modulo"] != "usuario:8" for r in rows))
        social = next(r for r in rows if r["id_evolucao_modulo"] == "usuario:3")
        self.assertEqual(social["demandas"], ["Documentos", "Saúde"])
        self.assertEqual(len(social["encaminhamentos"]), 2)
        self.assertEqual(social["possui_referencias_familiares"], "Sim")
        desligamento = next(r for r in rows if r["tipo_registro"] == "desligamento")
        self.assertEqual(desligamento["data_desligamento"], "2026-04-01")
        self.assertEqual(desligamento["motivo_desligamento_outros"], "Complemento")
        self.assertIsNone(social["motivo_desligamento_outros"])
        self.assertTrue(all(r["empregabilidade_imediata"] is None for r in rows))

    def test_participations_distinguish_activity_from_attendance_and_homonyms(self):
        rows = [r for r in self.read() if r["tipo_registro"] == "participacao"]
        self.assertEqual(len(rows), 3)
        self.assertEqual(len({r["id_atividade"] for r in rows}), 2)
        self.assertEqual({r["nome_atividade"] for r in rows}, {"Oficina"})
        self.assertEqual({r["hora_presenca"] for r in rows}, {"09:05"})
        self.assertTrue(all(r["id_unidade"] == 10 for r in rows))

    def test_conflicting_flags_are_visible_and_not_silently_selected(self):
        rows = self.read(self.fixtures(contradictory=True))
        self.assertGreater(
            next(r for r in rows if r["id_evolucao_modulo"] == "usuario:3")[
                "conflitos"
            ],
            0,
        )

    def test_scope_does_not_multiply_events_and_exposes_only_permitted_ids(self):
        fixtures = self.runner.fixtures_saida_enxuta()

        def row(activity=False):
            schema = dict(
                id_registro="STRING",
                id_usuario="INT64",
                id_unidade="INT64",
                origem="STRING",
                tipo_registro="STRING",
                abrangencia="STRING",
                id_evolucao_modulo="STRING",
                titulo_formulario="STRING",
                data_registro="DATETIME",
                data_resposta="DATETIME",
                **cpop_fields(),
                conflitos="INT64",
                nome_unidade="STRING",
            )
            values = dict(
                id_registro="presenca" if activity else "form",
                id_usuario=1,
                id_unidade=10,
                origem="presenca" if activity else "formulario_html",
                tipo_registro="participacao" if activity else "pai",
                abrangencia="unidade",
                id_evolucao_modulo=None if activity else "usuario:7",
                titulo_formulario="Origem",
                data_registro="2026-01-01 10:00:00",
                id_atividade=100 if activity else None,
                nome_atividade="Oficina" if activity else None,
                data_presenca="2026-01-01" if activity else None,
                hora_presenca="10:00" if activity else None,
                conflitos=0,
                nome_unidade="Centro POP A",
            )
            return fixture_rows(schema, [values])

        fixtures["int_pu_registros_centro_pop"] = row() + " union all " + row(True)
        result = self.runner.consultar_saida(fixtures)
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["atendimentos"]["quantidade"], 1)
        self.assertEqual(result[0]["evolucoes"]["quantidade"], 1)
        scope = result[0]["escopos"]["centro_pop"]
        self.assertNotIn("respostas", scope)
        self.assertEqual(scope["possui_registro_pai"], "Sim")
        self.assertEqual(scope["data_registro_pai"], "2026-01-01 10:00:00")
        self.assertEqual(scope["data_inclusao_pai"], "")
        self.assertEqual(scope["demandas_pai"], [])
        self.assertEqual(len(scope["historico"]), 1)
        self.assertEqual(scope["historico"][0]["data_inclusao_pai"], "")
        self.assertNotIn("id_registro", scope["historico"][0])
        self.assertEqual(scope["participacoes"][0]["id_atividade"], "100")
        empty = result[1]["escopos"]["centro_pop"]
        self.assertEqual(len(empty), 37)
        self.assertNotIn("respostas", empty)
        self.assertEqual(empty["historico"], [])
        self.assertEqual(empty["participacoes"], [])
        self.assertTrue(all(v in ("", []) for v in empty.values()))

    def test_form_only_user_is_included_without_service_attendance(self):
        fixtures = self.fixtures()
        original = fixtures["int_pu_formularios"]
        fixtures["int_pu_formularios"] = (
            original
            + " union all (select 3 id_usuario,formularios from ("
            + original
            + ") where formularios[offset(0)].titulo_formulario='Centro POP - Plano de Atendimento Individual (PAI)' limit 1)"
        )
        fixtures["dim_usuarios"] += " union all select 3,'Pessoa C'"
        rows = self.read(fixtures)
        self.assertEqual(len([r for r in rows if r["id_usuario"] == 3]), 1)

    def test_technical_classification_is_central_and_preserves_original_type(self):
        for kind, reception, expected in [
            ("2516", False, "Atendimento Técnico"),
            ("4110", False, "Outros Atendimentos"),
            ("4110", True, "Atendimento Recepção"),
        ]:
            with self.subTest(kind=kind, reception=reception):
                fixtures = self.runner.fixtures_multiplos_servicos()
                fixtures["int_pu_atendimentos"] = fixtures[
                    "int_pu_atendimentos"
                ].replace("['2516']", "['" + kind + "']")
                if reception:
                    fixtures["int_pu_atendimentos"] = fixtures[
                        "int_pu_atendimentos"
                    ].replace("'Alex'", "'ATENDIMENTO RECEPÇÃO'")
                rows = self.runner.consultar_saida(fixtures)
                events = rows[0]["atendimentos"]["registros"]
                cp = next(e for e in events if e["tipo_servico"] == "centro_pop")
                self.assertEqual(cp["classificacao_atendimento"], expected)
                self.assertEqual(cp["tipo_atendimento_descricao"], "Atendimento")
                self.assertTrue(
                    all(
                        e["classificacao_atendimento"] == ""
                        for e in events
                        if e["tipo_servico"] != "centro_pop"
                    )
                )

    def profile(self, records):
        return self.runner.consultar_modelo(
            "models/intermediate/planilha_unificada/dimensoes/dim_centro_pop.sql",
            {"int_pu_registros_centro_pop": records},
        )

    def test_latest_whole_form_across_units_with_stable_ties_and_empty_answers(self):
        schema = dict(
            id_usuario="INT64",
            id_registro="STRING",
            tipo_registro="STRING",
            id_unidade="INT64",
            data_registro="DATETIME",
            data_resposta="DATETIME",
            **cpop_fields(),
        )
        common = dict(
            id_usuario=1,
            data_registro="2026-01-01 10:00:00",
            data_resposta="2026-01-01 11:00:00",
        )
        records = fixture_rows(
            schema,
            [
                dict(
                    common,
                    id_registro="a",
                    tipo_registro="pai",
                    id_unidade=10,
                    data_inclusao_pai="2025-12-01",
                    observacoes="Antigo",
                    demandas=["Saúde"],
                ),
                dict(common, id_registro="z", tipo_registro="pai", id_unidade=20),
                dict(
                    common,
                    id_registro="zz",
                    tipo_registro="pai",
                    id_unidade=10,
                    data_resposta="2026-01-01 09:00:00",
                    observacoes="Resposta antiga",
                ),
                dict(
                    common,
                    id_registro="social",
                    tipo_registro="atendimento_social",
                    id_unidade=10,
                    possui_referencias_familiares="Sim",
                    observacoes="Social",
                ),
                dict(
                    common,
                    id_registro="saude",
                    tipo_registro="saude",
                    situacao_saude="Asma",
                ),
                dict(
                    common,
                    id_registro="rua",
                    tipo_registro="situacao_rua",
                    motivo_principal_permanencia_rua="Desemprego",
                ),
                dict(
                    common,
                    id_registro="desligamento",
                    tipo_registro="desligamento",
                    data_desligamento="2026-01-01",
                ),
                dict(
                    common, id_registro="p", tipo_registro="participacao", id_usuario=2
                ),
                dict(common, id_registro="s", tipo_registro="saude", id_usuario=3),
                dict(
                    common,
                    id_registro="null_date",
                    tipo_registro="pai",
                    data_registro=None,
                    observacoes="Sem data",
                ),
            ],
        )
        rows = self.profile(records)
        self.assertEqual([r["id_usuario"] for r in rows], [1, 3])
        row = rows[0]
        self.assertEqual(row["possui_registro_pai"], "Sim")
        self.assertIsNone(row["data_inclusao_pai"])
        self.assertEqual(row["observacoes_pai"], "")
        self.assertEqual(row["demandas_pai"], [])
        self.assertEqual(row["observacoes_atendimento_social"], "Social")
        self.assertEqual(row["situacao_saude"], "Asma")
        self.assertEqual(row["motivo_principal_permanencia_rua"], "Desemprego")
        self.assertEqual(row["data_desligamento"], "2026-01-01")
        self.assertEqual(rows[1]["possui_registro_pai"], "")
        self.assertTrue(all(not isinstance(v, dict) for r in rows for v in r.values()))
        self.assertNotIn("id_unidade", row)
        self.assertNotIn("acompanhamento_ativo", row)
        self.assertNotIn("empregabilidade_imediata", row)

    def test_referrals_are_sorted_distinct_text_and_stay_in_their_subject(self):
        schema = dict(
            id_usuario="INT64",
            id_registro="STRING",
            tipo_registro="STRING",
            data_registro="DATETIME",
            data_resposta="DATETIME",
            **cpop_fields(),
        )
        source = fixture_rows(
            schema,
            [dict(id_usuario=1, id_registro="a", tipo_registro="atendimento_social")],
        )
        records = (
            "select * replace([struct('Saúde' as label,'UBS' as valor),struct('Documentos','Cartório'),struct('Saúde','UBS')] as encaminhamentos) from ("
            + source
            + ")"
        )
        rows = self.profile(records)
        self.assertEqual(
            rows[0]["encaminhamentos_atendimento_social"],
            ["Documentos: Cartório", "Saúde: UBS"],
        )
        self.assertEqual(rows[0]["encaminhamentos_pai"], [])
