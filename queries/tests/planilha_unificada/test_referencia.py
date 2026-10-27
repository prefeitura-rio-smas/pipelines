"""Canonical PK regression and paired publication tests, without external data."""

# ruff: noqa: S608 - SQL composed only from local fixtures and validated relation helpers
import os
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import Mock, patch

from jinja2 import Environment, StrictUndefined
from test_modelos import QUERIES, fixture_rows

from queries.operations.planilha_unificada import quote, transaction_sql
from queries.operations.referencia_programada import (
    CANDIDATE,
    PUBLIC,
    Producer,
    destination,
    independent_parity_sql,
    parity_sql,
    redirect,
)


class RoutingTests(unittest.TestCase):
    def test_retry_does_not_rewrite_already_routed_query(self):
        producer = Producer.__new__(Producer)
        producer.get = Mock(
            return_value={
                "params": {
                    "query": "CREATE OR REPLACE TABLE " + CANDIDATE + " AS (SELECT 1)"
                }
            }
        )
        producer.active_runs = Mock(return_value=[])
        producer.patch = Mock()
        with TemporaryDirectory() as directory:
            producer.stage(Path(directory) / "backup.json")
        producer.patch.assert_called_once_with({"disabled": True})

    def test_redirect_changes_only_the_destination(self):
        original = "/* regra */ CREATE OR REPLACE TABLE " + PUBLIC + " AS (SELECT 1);"
        changed = redirect(original)
        self.assertEqual(destination(changed), CANDIDATE)
        self.assertEqual(changed.replace("`" + CANDIDATE + "`", PUBLIC), original)
        self.assertEqual(redirect(changed), changed)

    def test_unexpected_destination_is_rejected(self):
        with self.assertRaises(ValueError):
            redirect("CREATE OR REPLACE TABLE p.d.other AS (SELECT 1)")

    def test_independent_writer_blocks_publication(self):
        producer = Producer.__new__(Producer)
        producer.get = Mock(
            return_value={
                "params": {
                    "query": "CREATE OR REPLACE TABLE " + PUBLIC + " AS (SELECT 1)"
                }
            }
        )
        producer.active_runs = Mock(return_value=[])
        with self.assertRaisesRegex(ValueError, "ainda escreve"):
            producer.assert_routed()

    def test_old_pending_writer_blocks_publication_after_redirect(self):
        producer = Producer.__new__(Producer)
        producer.get = Mock(
            return_value={
                "params": {
                    "query": "CREATE OR REPLACE TABLE " + CANDIDATE + " AS (SELECT 1)"
                }
            }
        )
        producer.active_runs = Mock(
            return_value=[
                {
                    "params": {
                        "query": "CREATE OR REPLACE TABLE " + PUBLIC + " AS (SELECT 1)"
                    }
                }
            ]
        )
        with self.assertRaisesRegex(ValueError, "antiga"):
            producer.assert_routed()

    def test_missing_prefect_configuration_does_not_modify_producer(self):
        import asyncio

        from queries.operations import ativar_paridade

        settings = Mock()
        settings.api.url = None
        with (
            patch.object(
                ativar_paridade, "get_current_settings", return_value=settings
            ),
            patch("queries.operations.referencia_programada.Producer") as producer,
        ):
            with self.assertRaisesRegex(ValueError, "PREFECT_API_URL"):
                asyncio.run(ativar_paridade.activate())
            producer.assert_not_called()


@unittest.skipUnless(
    os.getenv("RUN_BIGQUERY_TESTS") == "1", "Optional synthetic BigQuery tests"
)
class ReferenceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from google.cloud import bigquery

        cls.client = bigquery.Client(
            project=os.getenv("BIGQUERY_TEST_PROJECT", "rj-smas-dev")
        )

    def test_reference_controls_membership_cancellation_modules_and_professionals(self):
        schema = {
            "seq_atendimento": "int64",
            "modulo": "string",
            "id_usuario": "int64",
            "unidade_atendimento": "string",
            "nome_atendimento_original": "string",
            "data_de_atendimento": "date",
            "hora_de_atendimento": "string",
            "profissional_id": "int64",
            "profissional": "string",
            "profissional_cbo_original": "string",
        }
        base = {
            "seq_atendimento": 7,
            "modulo": "u",
            "id_usuario": 1,
            "unidade_atendimento": "CRAS A",
            "nome_atendimento_original": "Atendimento",
            "data_de_atendimento": "2026-10-26",
            "hora_de_atendimento": "9:05",
            "profissional_id": 100,
            "profissional": "Pessoa A",
            "profissional_cbo_original": "Assistente social",
        }
        fixtures = {
            "atendimentos_programada": fixture_rows(
                schema,
                [
                    base,
                    base,
                    dict(base, profissional_id=101),
                    dict(base, modulo="f", profissional_id=None),
                ],
            )
        }
        raw_schema = {
            "id_atendimento_modulo": "string",
            "id_unidade": "int64",
            "id_tipo_atendimento": "int64",
            "flag_cancelado": "string",
        }
        fixtures.update(
            raw_atendimentos_usuarios=fixture_rows(
                raw_schema,
                [
                    {
                        "id_atendimento_modulo": "7u",
                        "id_unidade": 10,
                        "id_tipo_atendimento": 1,
                        "flag_cancelado": "S",
                    },
                    {"id_atendimento_modulo": "8u", "id_unidade": 10},
                ],
            ),
            raw_atendimentos_familias=fixture_rows(
                raw_schema,
                [
                    {
                        "id_atendimento_modulo": "7f",
                        "id_unidade": 10,
                        "id_tipo_atendimento": 1,
                    }
                ],
            ),
            dim_unidades="select 10 id_unidade,'CRAS' tipo_unidade",
            raw_profissionais_ocupacoes="select 100 id_profissional,251605 codigo_cbo",
            raw_cbo="select 251605 codigo_cbo,'Assistente social' descricao",
        )
        env = Environment(undefined=StrictUndefined, autoescape=False)  # noqa: S701

        def relation(name):
            return "(" + fixtures[name] + ")"

        env.globals.update(
            execute=False,
            source=lambda *args: relation("atendimentos_programada"),
            pu_ref=relation,
            pu_tipo_servico=lambda expr: (
                "case " + expr + " when 'CRAS' then 'cras' else 'outros' end"
            ),
        )
        env.globals["normalizar_hora_atendimento"] = env.from_string(
            (QUERIES / "macros/utils/normalizar_hora_atendimento.sql").read_text()
        ).module.normalizar_hora_atendimento
        sql = env.from_string(
            (QUERIES / "macros/dominios/planilha_unificada/referencia.sql").read_text()
        ).module.pu_atendimentos_referencia_sql()
        rows = list(self.client.query(sql).result())
        events = {r["id_atendimento_modulo"]: r for r in rows[0].atendimentos}
        self.assertEqual(set(events), {"7u", "7f"})
        self.assertEqual(events["7u"]["flag_cancelado"], "S")
        self.assertEqual(events["7u"]["hora_atendimento"], "09:05")
        self.assertEqual(len(events["7u"]["profissionais"]), 2)
        self.assertEqual(events["7f"]["profissionais"], [])

    def setup_sql(self, invalid=False):
        key = "2u" if invalid else "1u"
        return (
            """
        CREATE TEMP TABLE referencia AS SELECT 1 SEQ_ATENDIMENTO,'u' modulo,1 ID_USUARIO,
          DATE '2026-10-26' DATA_DE_ATENDIMENTO,'09:00' HORA_DE_ATENDIMENTO,
          'CRAS A' UNIDADE_ATENDIMENTO,'Atendimento' NOME_ATENDIMENTO_ORIGINAL,prof PROFISSIONAL_ID
          FROM UNNEST([100,101]) prof;
        CREATE TEMP TABLE candidato AS SELECT STRUCT('1' AS id_usuario) usuario,
          STRUCT(1 AS quantidade,STRUCT(1 AS cras,0 AS creas,0 AS centro_pop,0 AS outros) AS quantidade_por_servico,
            [STRUCT('"""
            + key
            + """' AS id_atendimento_modulo)] AS registros) atendimentos;
        CREATE TEMP TABLE destino AS SELECT STRUCT('1' AS id_usuario) usuario,
          STRUCT(1 AS quantidade,STRUCT(1 AS cras,0 AS creas,0 AS centro_pop,0 AS outros) AS quantidade_por_servico,
            [STRUCT('8u' AS id_atendimento_modulo)] AS registros) atendimentos;
        CREATE TEMP TABLE programada AS SELECT r.* REPLACE(8 AS SEQ_ATENDIMENTO) FROM referencia r;
        CREATE TEMP TABLE controle AS SELECT servico,'antigo' lote,'antigo' execucao,
          CURRENT_TIMESTAMP() capturado_em,CURRENT_TIMESTAMP() publicado_em,5 geracao,'pronto' estado,'v0' referencia_versao
          FROM UNNEST(['supertabela','programada','cras','creas','centro_pop','outros']) servico;
        """
        )

    def transaction(self):
        info = {
            "lote": "lote",
            "execucao": "execucao",
            "capturado_em": "2026-10-26T00:00:00+00:00",
        }
        return transaction_sql(
            "controle",
            "destino",
            "candidato",
            [],
            5,
            {"programada": info},
            info,
            reference="referencia",
            programada="programada",
            reference_version="v1",
        )

    def test_paired_transaction_publishes_same_pk_despite_two_professionals(self):
        sql = (
            self.setup_sql()
            + self.transaction()
            + "SELECT (SELECT COUNT(*) FROM programada) linhas,(SELECT atendimentos.quantidade FROM destino) eventos,(SELECT geracao FROM controle WHERE servico='supertabela') geracao;"
        )
        row = list(self.client.query(sql).result())[0]
        self.assertEqual((row.linhas, row.eventos, row.geracao), (2, 1, 6))

    def test_same_count_but_wrong_pk_rolls_back_both_outputs(self):
        sql = (
            self.setup_sql(True)
            + "BEGIN\n"
            + self.transaction()
            + """
        EXCEPTION WHEN ERROR THEN ROLLBACK TRANSACTION; END;
        SELECT (SELECT MIN(SEQ_ATENDIMENTO) FROM programada) seq,
          (SELECT atendimentos.registros[OFFSET(0)].id_atendimento_modulo FROM destino) chave,
          (SELECT geracao FROM controle WHERE servico='supertabela') geracao;
        """
        )
        row = list(self.client.query(sql).result())[0]
        self.assertEqual((row.seq, row.chave, row.geracao), (8, "8u", 5))

    def test_conflicting_reference_attributes_are_rejected(self):
        sql = (
            self.setup_sql()
            + "INSERT INTO referencia SELECT r.* REPLACE(2 AS ID_USUARIO) FROM referencia r LIMIT 1;"
            + parity_sql("referencia", "candidato", quote)
        )
        from google.api_core.exceptions import BadRequest

        with self.assertRaises(BadRequest):
            self.client.query(sql).result()

    def independent_fixture(self, invalid=False):
        manifest = {
            s: {
                "base_schema": "nova" if s == "cras" else "antiga",
                "lote": "nova" if s == "cras" else "antiga",
                "execucao": "teste",
                "capturado_em": "2026-10-26T00:00:00+00:00",
                "referencia_versao": "v_nova" if s == "cras" else "v_antiga",
            }
            for s in ["cras", "creas", "centro_pop", "outros"]
        }
        dimensions = {s: "dim_" + s for s in manifest}
        setup = """
          CREATE TEMP TABLE unidades AS SELECT 10 id_unidade,'CRAS' tipo_unidade UNION ALL SELECT 20,'CREAS';
          CREATE TEMP TABLE usuarios AS SELECT 1 id_usuario UNION ALL SELECT 2;
          CREATE TEMP TABLE origens AS SELECT CONCAT(CAST(seq AS STRING),'u') id_atendimento_modulo,
            IF(seq IN (1,8),10,20) id_unidade FROM UNNEST([1,2,8,9]) seq;
          CREATE TEMP TABLE familias AS SELECT * FROM origens WHERE FALSE;
          CREATE TEMP TABLE nova_ref AS SELECT 1 ID_USUARIO,1 SEQ_ATENDIMENTO,'u' modulo UNION ALL SELECT 2,9,'u';
          CREATE TEMP TABLE antiga_ref AS SELECT 1 ID_USUARIO,8 SEQ_ATENDIMENTO,'u' modulo UNION ALL SELECT 2,2,'u';
          CREATE TEMP TABLE nova_evo AS SELECT 1 id_usuario,CAST(NULL AS INT64) id_paciente_familia,
            10 id_unidade,7 id_evolucao,'usuario' origem_modulo,CAST(NULL AS DATETIME) data_cancelamento;
          CREATE TEMP TABLE antiga_evo AS SELECT e.* REPLACE(2 AS id_usuario,20 AS id_unidade) FROM nova_evo e;
          CREATE TEMP TABLE dim_cras AS SELECT 1 id_usuario,[STRUCT('1u' AS id_atendimento_modulo)] atendimentos,
            [STRUCT('usuario:7' AS id_evolucao_modulo)] evolucoes;
          CREATE TEMP TABLE dim_creas AS SELECT d.* REPLACE(2 AS id_usuario,[STRUCT('2u' AS id_atendimento_modulo)] AS atendimentos) FROM dim_cras d;
          CREATE TEMP TABLE dim_centro_pop AS SELECT * FROM dim_cras WHERE FALSE;
          CREATE TEMP TABLE dim_outros AS SELECT * FROM dim_cras WHERE FALSE;
          CREATE TEMP TABLE candidato AS SELECT STRUCT(CAST(d.id_usuario AS STRING) AS id_usuario) usuario,
            STRUCT(1 AS quantidade,STRUCT(IF(d.id_usuario=1,1,0) AS cras,IF(d.id_usuario=2,1,0) AS creas,0 AS centro_pop,0 AS outros) AS quantidade_por_servico,
              [STRUCT(d.atendimentos[OFFSET(0)].id_atendimento_modulo AS id_atendimento_modulo,IF(d.id_usuario=1,'cras','creas') AS tipo_servico)] AS registros) atendimentos,
            STRUCT(1 AS quantidade,STRUCT(IF(d.id_usuario=1,1,0) AS cras,IF(d.id_usuario=2,1,0) AS creas,0 AS centro_pop,0 AS outros) AS quantidade_por_servico,
              [STRUCT('usuario:7' AS id_evolucao_modulo,IF(d.id_usuario=1,'cras','creas') AS tipo_servico)] AS registros) evolucoes
            FROM (SELECT * FROM dim_cras UNION ALL SELECT * FROM dim_creas) d;
          CREATE TEMP TABLE destino AS SELECT * FROM candidato;
          UPDATE destino SET atendimentos.registros=[STRUCT('8u' AS id_atendimento_modulo,'cras' AS tipo_servico)] WHERE usuario.id_usuario='1';
          CREATE TEMP TABLE cras_publicada AS SELECT d.* REPLACE([STRUCT('8u' AS id_atendimento_modulo)] AS atendimentos) FROM dim_cras d;
          CREATE TEMP TABLE controle AS SELECT servico,'antiga' lote,'antiga' execucao,
            CURRENT_TIMESTAMP() capturado_em,CURRENT_TIMESTAMP() publicado_em,5 geracao,'pronto' estado,
            'v_antiga' referencia_versao,CAST(NULL AS JSON) dimensoes_utilizadas
            FROM UNNEST(['supertabela','cras','creas','centro_pop','outros']) servico;
        """
        if invalid:
            setup += "UPDATE candidato SET atendimentos.registros=[STRUCT('9u' AS id_atendimento_modulo,'creas' AS tipo_servico)] WHERE usuario.id_usuario='2';"
        published = {**dimensions, "cras": "cras_publicada"}
        checks = independent_parity_sql(
            "p", manifest, published, "destino", "usuarios", quote
        )
        for schema, prefix in [("nova", "nova"), ("antiga", "antiga")]:
            for name, fixture in {
                "atendimentos_programada": prefix + "_ref",
                "int_evolucoes": prefix + "_evo",
                "raw_atendimentos_usuarios": "origens",
                "raw_atendimentos_familias": "familias",
                "dim_unidades": "unidades",
                "dim_usuarios": "usuarios",
            }.items():
                checks = checks.replace(f"`p.{schema}.{name}`", fixture)
        transaction = transaction_sql(
            "controle",
            "destino",
            "candidato",
            [("cras_publicada", "dim_cras")],
            5,
            {"cras": manifest["cras"]},
            manifest["cras"],
            checks=checks,
            dimension_manifest=manifest,
        )
        return setup, transaction

    def test_independent_versions_preserve_other_services_and_shared_evolution_ids(
        self,
    ):
        setup, transaction = self.independent_fixture()
        row = list(
            self.client.query(
                setup
                + transaction
                + """
          SELECT (SELECT SUM(atendimentos.quantidade) FROM destino) atendimentos,
            (SELECT SUM(evolucoes.quantidade) FROM destino) evolucoes,
            (SELECT lote FROM controle WHERE servico='creas') lote_creas,
            (SELECT referencia_versao FROM controle WHERE servico='supertabela') referencia_global,
            (SELECT JSON_VALUE(dimensoes_utilizadas,'$.creas.lote') FROM controle WHERE servico='supertabela') manifesto_creas;
        """
            ).result()
        )[0]
        self.assertEqual(
            (
                row.atendimentos,
                row.evolucoes,
                row.lote_creas,
                row.referencia_global,
                row.manifesto_creas,
            ),
            (2, 2, "antiga", None, "antiga"),
        )

    def test_invalid_independent_candidate_rolls_back_selected_dimension_and_root(self):
        setup, transaction = self.independent_fixture(invalid=True)
        row = list(
            self.client.query(
                setup
                + "BEGIN\n"
                + transaction
                + """
          EXCEPTION WHEN ERROR THEN ROLLBACK TRANSACTION; END;
          SELECT (SELECT atendimentos[OFFSET(0)].id_atendimento_modulo FROM cras_publicada) chave_dim,
            (SELECT atendimentos.registros[OFFSET(0)].id_atendimento_modulo FROM destino WHERE usuario.id_usuario='1') chave_raiz,
            (SELECT geracao FROM controle WHERE servico='supertabela') geracao;
        """
            ).result()
        )[0]
        self.assertEqual((row.chave_dim, row.chave_raiz, row.geracao), ("8u", "8u", 5))
