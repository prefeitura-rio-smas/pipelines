"""Regressions for the frozen BI photo and additional address snapshot."""

# ruff: noqa: S608 - local synthetic SQL fixtures only
import os
import unittest

import test_modelos as base
from test_modelos import bi_reference_schema, fixture_rows


@unittest.skipUnless(os.getenv("RUN_BIGQUERY_TESTS") == "1", "Synthetic BigQuery SQL")
class BiComplementsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from google.cloud import bigquery

        cls.runner = base.PlanilhaUnificadaTests()
        cls.runner.client = bigquery.Client(
            project=os.getenv("BIGQUERY_TEST_PROJECT", "rj-smas-dev")
        )

    def fixtures(self):
        return {
            "dim_usuarios": "select id_usuario from unnest([1,2,3,4]) id_usuario",
            "atendimentos_programada": fixture_rows(
                bi_reference_schema(),
                [
                    dict(
                        id_usuario=1,
                        seq_atendimento=10,
                        modulo="u",
                        cas="01",
                        profissional_id=7,
                        id_familia=100,
                        id_atendimento="10-7",
                        cadastrante="OPERADOR",
                        data_cadastro_atendimento="2026-01-02",
                        endereco="Rua da foto",
                        endereco_numero="001",
                    ),
                    dict(
                        id_usuario=1,
                        seq_atendimento=10,
                        modulo="u",
                        cas="01",
                        profissional_id=8,
                        id_familia=200,
                        id_atendimento="10-8",
                        cadastrante="OPERADOR",
                        data_cadastro_atendimento="2026-01-02",
                        endereco="Rua da foto",
                        endereco_numero="001",
                    ),
                    dict(
                        id_usuario=1,
                        seq_atendimento=10,
                        modulo="u",
                        cas="01",
                        profissional_id=None,
                        id_familia=300,
                        cadastrante="OPERADOR",
                        data_cadastro_atendimento="2026-01-02",
                        endereco="Rua da foto",
                        endereco_numero="001",
                    ),
                    dict(
                        id_usuario=2,
                        seq_atendimento=10,
                        modulo="f",
                        profissional_id=7,
                        id_familia=400,
                        id_atendimento="10-7",
                    ),
                ],
            ),
            "raw_usuarios_enderecos": fixture_rows(
                dict(
                    id_paciente="INT64",
                    logradouro="STRING",
                    numero="STRING",
                    complemento="STRING",
                    referencia_ou_comunidade="STRING",
                ),
                [
                    dict(
                        id_paciente=1,
                        logradouro="Cadastro posterior",
                        numero="99",
                        complemento="Não preencher",
                    ),
                    dict(id_paciente=2, logradouro="Foto inteira vazia"),
                    dict(
                        id_paciente=3,
                        logradouro="Rua cadastro",
                        numero="0",
                        complemento="Fundos",
                    ),
                ],
            ),
        }

    def test_whole_address_photo_wins_and_other_users_use_frozen_registry(self):
        rows = self.runner.consultar_modelo(
            "models/intermediate/planilha_unificada/cadastro/int_pu_enderecos_bi.sql",
            self.fixtures(),
        )
        self.assertEqual(len(rows), 4)
        self.assertEqual(
            rows[0]["endereco"],
            dict(
                logradouro="Rua da foto",
                numero="001",
                complemento=None,
                referencia_ou_comunidade=None,
            ),
        )
        self.assertEqual(rows[1]["origem_endereco"], "referencia_bi")
        self.assertTrue(all(v is None for v in rows[1]["endereco"].values()))
        self.assertEqual(rows[2]["origem_endereco"], "cadastro")
        self.assertEqual(rows[2]["endereco"]["numero"], "0")
        self.assertEqual(rows[3]["origem_endereco"], "sem_registro")

    def test_family_context_is_per_professional_and_modules_do_not_collide(self):
        rows = self.runner.consultar_modelo(
            "models/intermediate/planilha_unificada/atendimentos/int_pu_complementos_bi.sql",
            self.fixtures(),
        )
        by_key = {r["id_atendimento_modulo"]: r for r in rows}
        self.assertEqual(set(by_key), {"10u", "10f"})
        self.assertEqual(by_key["10u"]["data_cadastro_atendimento"], "2026-01-02")
        self.assertEqual(
            by_key["10u"]["profissionais"],
            [
                dict(
                    id_profissional=7,
                    id_familia_referencia=100,
                    id_atendimento_legado="10-7",
                ),
                dict(
                    id_profissional=8,
                    id_familia_referencia=200,
                    id_atendimento_legado="10-8",
                ),
            ],
        )
        self.assertEqual(
            by_key["10f"]["profissionais"][0]["id_familia_referencia"], 400
        )

    def test_conflicting_common_context_is_not_silently_chosen(self):
        fixtures = self.fixtures()
        old = fixtures["atendimentos_programada"]
        fixtures["atendimentos_programada"] = (
            old
            + " union all select * replace('02' as cas) from ("
            + old
            + ") where profissional_id=8"
        )
        rows = self.runner.consultar_modelo(
            "models/intermediate/planilha_unificada/atendimentos/int_pu_complementos_bi.sql",
            fixtures,
        )
        self.assertEqual(
            len([r for r in rows if r["id_atendimento_modulo"] == "10u"]), 2
        )

    def test_address_cleaning_preserves_zero_and_leading_zero(self):
        schema = dict(
            seqpac="INT64",
            dslogradouro="STRING",
            numend="STRING",
            complend="STRING",
            pontorefe="STRING",
        )
        rows = self.runner.consultar_modelo(
            "models/raw/prontuario_carioca_assistencia_social/raw_usuarios_enderecos.sql",
            {
                "gh_cidadao_pac": fixture_rows(
                    schema,
                    [
                        dict(
                            seqpac=1,
                            dslogradouro=" Rua,  A; ",
                            numend="001",
                            complend="0",
                            pontorefe="Perto; escola",
                        )
                    ],
                )
            },
        )
        self.assertEqual(rows[0]["logradouro"], "Rua A")
        self.assertEqual(rows[0]["numero"], "001")
        self.assertEqual(rows[0]["complemento"], "0")
        self.assertEqual(rows[0]["referencia_ou_comunidade"], "Perto escola")

    def test_public_output_adds_complements_without_fanout_or_phantom_professionals(
        self,
    ):
        fixtures = self.runner.fixtures_saida_enxuta()
        fixtures["int_pu_enderecos_bi"] = (
            """select id_usuario,struct('Rua' as logradouro,cast(null as string) as numero,cast(null as string) as complemento,cast(null as string) as referencia_ou_comunidade) endereco from unnest([1,2]) id_usuario"""
        )
        # Match the synthetic event/professional IDs without assuming fixture values.
        fixtures["int_pu_complementos_bi"] = (
            """select p.id_usuario,a.id_atendimento_modulo,'01' cas,'unidade' email_unidade,'cas' email_cas,'literal,,email' email,'CADASTRANTE' cadastrante, date '2025-12-01' data_cadastro_atendimento,
          array(select as struct pr.id_profissional,100 id_familia_referencia,'legado' id_atendimento_legado from unnest(a.profissionais) pr) profissionais
          from ("""
            + fixtures["int_pu_atendimentos"]
            + """ ) p,unnest(p.atendimentos) a"""
        )
        rows = self.runner.consultar_saida(fixtures)
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["atendimentos"]["quantidade"], 1)
        self.assertEqual(rows[0]["evolucoes"]["quantidade"], 1)
        a = rows[0]["atendimentos"]["registros"][0]
        self.assertEqual(a["email"], "literal,,email")
        self.assertEqual(a["data_cadastro_atendimento"], "2025-12-01")
        self.assertTrue(
            all(
                pr["id_atendimento_legado"] == "legado"
                and pr["id_familia_referencia"] == "100"
                for pr in a["profissionais"]
            )
        )
        self.assertEqual(rows[1]["atendimentos"]["registros"], [])
        self.assertEqual(rows[0]["usuario"]["endereco"]["numero"], "")
