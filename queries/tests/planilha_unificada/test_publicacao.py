# ruff: noqa: S608 - queries assembled exclusively from local synthetic fixtures
"""Publication failure paths; no credentials needed unless RUN_BIGQUERY_TESTS=1."""

from contextlib import nullcontext
import importlib.util
import json
import os
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import Mock, patch
from uuid import uuid4

from test_campos_diretos import old_schema as v8_schema
from test_campos_diretos import old_select_sql

spec = importlib.util.spec_from_file_location(
    "pu_operacao",
    Path(__file__).resolve().parents[2] / "operations/planilha_unificada.py",
)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class PublicationTests(unittest.TestCase):
    def pipeline(self):
        p = module.Pipeline.__new__(module.Pipeline)
        p.project, p.target, p.core = "rj-smas-dev", "staging", "gerenciamento__dbt"
        p.control, p.destination = "p.d.controle", "p.d.supertabela"
        p.lock = Mock(return_value=nullcontext())
        p.dataset, p.verify, p.initialize_control = Mock(), Mock(), Mock()
        p.query, p.write_json, p.dbt = Mock(), Mock(), Mock()
        p.validate_candidate_schema, p.check_derived_batch = Mock(), Mock()
        return p

    def test_parse_keeps_events_independent_from_legacy_dimensions(self):
        from jinja2 import Environment, StrictUndefined

        references = []

        def reference(name):
            references.append(name)
            return name

        env = Environment(undefined=StrictUndefined, autoescape=False)  # noqa: S701 - SQL, not HTML
        env.globals.update(
            execute=False,
            model={"resource_type": "test"},
            ref=reference,
            var=lambda name, default=None: (
                {"cras": "cras_candidata"} if name == "pu_dimensoes" else default
            ),
        )
        macros = env.from_string(
            (
                module.ROOT / "macros/dominios/planilha_unificada/dimensoes.sql"
            ).read_text()
        ).module
        macros.pu_ref("int_pu_atendimentos")
        self.assertEqual(references, ["int_pu_atendimentos"])

    def test_selection_keeps_all_attached_tests_without_selecting_final_table(self):
        p = self.pipeline()
        nodes = {
            "preservacao": {
                "resource_type": "test",
                "attached_node": "model.queries.dim_cras",
                "fqn": ["queries", "preservacao"],
                "depends_on": {
                    "nodes": [
                        "model.queries.dim_cras",
                        "model.queries.int_pu_atendimentos",
                    ]
                },
            },
            "root": {
                "resource_type": "test",
                "attached_node": None,
                "fqn": ["queries", "root"],
                "depends_on": {
                    "nodes": [
                        "model.queries.planilha_unificada",
                        "model.queries.dim_cras",
                    ]
                },
            },
        }
        with TemporaryDirectory() as directory:
            artifact = Path(directory)
            (artifact / "manifest.json").write_text(json.dumps({"nodes": nodes}))
            self.assertEqual(
                p.tests_for(artifact, ["dim_cras"]), ["fqn:queries.preservacao"]
            )
            self.assertEqual(
                p.tests_for(artifact, ["planilha_unificada"]), ["fqn:queries.root"]
            )

    def test_invalid_identifiers_never_become_sql(self):
        for value in ["../prod", "a;delete", "abc`", "UPPER", "a-b", ""]:
            with self.assertRaises(ValueError):
                module.identifier(value)
        with self.assertRaises(ValueError):
            module.quote("p.d.table`; DROP TABLE x")

    def test_failed_dbt_does_not_mark_dimension_valid(self):
        p = self.pipeline()
        p.context = Mock(
            return_value=(
                "pu_cand_teste",
                {},
                {
                    "pu_fontes": dict.fromkeys(
                        [
                            "raw_presencas_usuarios",
                            "raw_evolucoes_questionario",
                            "raw_evolucoes_questionario_lista",
                        ],
                        True,
                    )
                },
            )
        )
        p.read_json = Mock(side_effect=module.NotFound("not built"))
        p.dbt.side_effect = RuntimeError("test failed")
        with self.assertRaises(RuntimeError):
            p.build_dimension("lote", "teste", "centro_pop")
        p.write_json.assert_not_called()

    def test_scope_consolidation_has_no_legacy_event_dimensions(self):
        p = self.pipeline()
        p.context = Mock(
            return_value=(
                "pu_cand_teste",
                {"lote": "lote"},
                {"pu_referencia": True, "pu_fontes": {"raw_usuarios_enderecos": True}},
            )
        )
        p.read_json = Mock(
            return_value={
                "validacao": module.SCOPE_CONTRACT,
                "lote": "lote",
                "tabelas": {
                    "p.d.dim_centro_pop": {},
                    "p.d.int_pu_registros_centro_pop": {},
                },
            }
        )
        p.state = Mock(return_value={"supertabela": {"geracao": 4}})
        p.base = Mock(
            return_value=(
                "pu_base_lote",
                {
                    "referencia": {"versao": "v"},
                    "capturado_em": "2026-10-27T00:00:00+00:00",
                },
            )
        )
        p.fingerprint = Mock(return_value={"rows": 1})
        p.migration_baseline = Mock(
            return_value={
                "tabela": "p.d.antes",
                "fingerprint": {"schema": v8_schema()},
                "publicadas": {},
            }
        )
        p.consolidate("lote", "teste", ["centro_pop"])
        metadata = p.write_json.call_args.args[2]
        self.assertEqual(metadata["contrato"], module.UNIFIED_CONTRACT)
        self.assertTrue(metadata["publicacao_permitida"])
        self.assertEqual(set(metadata["escopos"]), {"centro_pop"})
        self.assertNotIn("pu_dimensoes", p.dbt.call_args.args[2])
        self.assertNotIn("dim_cras", str(p.query.call_args_list))

    def test_legacy_scope_artifact_and_other_batch_are_rejected(self):
        for contract, batch in [
            ("programada_v2", "lote"),
            (module.SCOPE_CONTRACT, "outro"),
        ]:
            p = self.pipeline()
            p.context = Mock(return_value=("pu_cand_teste", {}, {}))
            p.read_json = Mock(return_value={"validacao": contract, "lote": batch})
            with self.assertRaisesRegex(ValueError, "legado ou de outro lote"):
                p.consolidate("lote", "teste", ["centro_pop"])
            p.dbt.assert_not_called()

    def test_legacy_dimensions_cannot_be_built_by_scope_operator(self):
        p = self.pipeline()
        p.context = Mock()
        for service in ["cras", "creas", "outros"]:
            with self.assertRaisesRegex(ValueError, "legadas"):
                p.build_dimension("lote", "teste", service)
        p.context.assert_not_called()

    def test_publication_requires_explicit_new_contract_migration(self):
        p = self.pipeline()
        p.read_json = Mock(return_value={"contrato": "centro_pop_contexto_v2"})
        p.copy = Mock()
        with self.assertRaisesRegex(ValueError, "nova consolidação"):
            p.publish("teste")
        p.copy.assert_not_called()

    def migration_pipeline(self):
        p = self.pipeline()
        p.assert_paused, p.copy = Mock(), Mock()
        p.base = Mock(return_value=("pu_base_lote", {}))
        p.state = Mock(return_value={"supertabela": {"geracao": 4}})
        p.read_json = Mock(
            return_value={
                "contrato": module.UNIFIED_CONTRACT,
                "publicacao_permitida": True,
                "geracao": 4,
                "lote": "lote",
                "tabela": {},
                "tabelas": {},
                "antes_migracao": {
                    "tabela": "p.d.antes",
                    "fingerprint": {"schema": v8_schema()},
                    "publicadas": {},
                },
                "cadastro": {"capturado_em": "2026-10-27T00:00:00+00:00"},
                "referencia": {"versao": "v"},
                "escopos": {},
            }
        )
        return p

    def test_migration_requires_same_frozen_batch(self):
        p = self.pipeline()
        p.base = Mock(return_value=("pu_base_lote", {}))
        with self.assertRaisesRegex(ValueError, "mesmo lote congelado"):
            p.migration_baseline("schema", "lote", {"supertabela": {"lote": "outro"}})

    def test_stale_generation_and_active_schedule_prevent_replacement(self):
        p = self.migration_pipeline()
        p.state.return_value = {"supertabela": {"geracao": 5}}
        with self.assertRaisesRegex(ValueError, "Geração mudou"):
            p.publish("teste")
        p.copy.assert_not_called()
        p = self.migration_pipeline()
        p.assert_paused.side_effect = ValueError("Programada ativa")
        with self.assertRaisesRegex(ValueError, "ativa"):
            p.publish("teste")
        p.copy.assert_not_called()

    def test_all_backups_precede_live_writes_and_only_centro_pop_is_migrated(self):
        p = self.migration_pipeline()
        p.publish("teste")
        copies = [c.args for c in p.copy.call_args_list]
        self.assertEqual(len(copies), 3)
        self.assertTrue(
            all("_pu_backup_" in destination for _, destination in copies[:2])
        )
        self.assertEqual(
            {d for _, d in copies[2:]},
            {p.destination},
        )
        self.assertEqual(p.write_json.call_args.args[2]["estado"], "publicado")
        self.assertEqual(p.assert_paused.call_count, 3)

    def test_failure_after_first_live_write_restores_each_attempted_table(self):
        p = self.migration_pipeline()
        count = 0

        def copy(source, destination):
            nonlocal count
            count += 1
            if count == 3:
                raise RuntimeError("copy failed")

        p.copy.side_effect = copy
        with self.assertRaisesRegex(RuntimeError, "copy failed"):
            p.publish("teste")
        copies = [c.args for c in p.copy.call_args_list]
        self.assertEqual(len(copies), 4)
        self.assertTrue(all("_pu_backup_" in source for source, _ in copies[-1:]))
        self.assertEqual(p.write_json.call_args.args[2]["estado"], "restaurado")

    def test_failed_restoration_is_recorded_with_recovery_backups(self):
        p = self.migration_pipeline()
        count = 0

        def copy(source, destination):
            nonlocal count
            count += 1
            if count >= 3:
                raise RuntimeError("copy failed")

        p.copy.side_effect = copy
        with self.assertRaisesRegex(RuntimeError, "Falha na restauração"):
            p.publish("teste")
        recovery = p.write_json.call_args.args[2]
        self.assertEqual(recovery["estado"], "restauracao_pendente")
        self.assertEqual(len(recovery["backups"]), 2)
        self.assertEqual(len(recovery["falhas_restauracao"]), 1)

    def test_cleanup_preserves_backups_of_pending_migrations(self):
        for status in ["preparado", "restauracao_pendente"]:
            p = self.pipeline()
            p.state = Mock(return_value={})
            p.client = Mock()
            p.client.list_datasets.return_value = []
            p.client.list_tables.return_value = []
            p.read_json = Mock(return_value={"estado": status})
            with self.assertRaisesRegex(ValueError, "backups de migração pendente"):
                p.clean(["pu_cand_teste"])
            p.client.delete_dataset.assert_not_called()

    def test_paused_check_also_rejects_pending_runs(self):
        p = self.pipeline()
        with patch.object(module, "Producer") as producer:
            producer.return_value.get.return_value = {"disabled": True}
            producer.return_value.active_runs.return_value = [{"state": "PENDING"}]
            with self.assertRaisesRegex(ValueError, "pendentes"):
                p.assert_paused()

    def test_automatic_cycle_is_disabled_before_cloud_access(self):
        p = self.pipeline()
        with self.assertRaisesRegex(ValueError, "automático desabilitado"):
            p.cycle()
        p.query.assert_not_called()

    def test_only_additive_service_fields_can_migrate(self):
        def schema(extra=None):
            return [
                module.bigquery.SchemaField(
                    block,
                    "RECORD",
                    fields=[
                        module.bigquery.SchemaField(
                            "registros",
                            "RECORD",
                            mode="REPEATED",
                            fields=[
                                module.bigquery.SchemaField(key, "STRING"),
                                *(
                                    [module.bigquery.SchemaField(extra, "STRING")]
                                    if extra
                                    else []
                                ),
                            ],
                        )
                    ],
                )
                for block, key in [
                    ("atendimentos", "id_atendimento_modulo"),
                    ("evolucoes", "id_evolucao_modulo"),
                ]
            ]

        for extra, valid in [("tipo_servico", True), ("campo_inesperado", False)]:
            with self.subTest(extra=extra):
                p = self.pipeline()
                p.client = Mock()
                p.client.get_table.side_effect = [
                    Mock(schema=schema()),
                    Mock(schema=schema(extra)),
                ]
                if valid:
                    self.assertTrue(p.ensure_public_schema("p.d.candidato"))
                    p.client.update_table.assert_called_once()
                else:
                    with self.assertRaisesRegex(ValueError, "não aditiva"):
                        p.ensure_public_schema("p.d.candidato")
                    p.client.update_table.assert_not_called()

    def test_reclassification_blocks_single_service_but_allows_joint_selection(self):
        p = self.pipeline()
        p.query.return_value = [{"anterior": "cras", "novo": "creas"}]
        with self.assertRaisesRegex(ValueError, "juntas"):
            p.check_movements("pu_base_teste", {"cras": "p.d.cras"}, ["cras"])
        sql = p.query.call_args.args[0]
        self.assertIn("AND NOT (a.servico IN ('cras') AND n.servico IN ('cras'))", sql)
        p.query.return_value = []
        p.check_movements("pu_base_teste", {"cras": "p.d.cras"}, ["cras", "creas"])
        self.assertIn("IN ('cras','creas')", p.query.call_args.args[0])


@unittest.skipUnless(
    os.getenv("RUN_BIGQUERY_TESTS") == "1",
    "Optional BigQuery temporary-table transaction tests",
)
class TransactionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.client = module.bigquery.Client(
            project=os.getenv("BIGQUERY_TEST_PROJECT", "rj-smas-dev")
        )

    def setup_sql(self):
        return """
        CREATE TEMP TABLE controle AS SELECT servico, 'anterior' lote, 'anterior' execucao,
          CURRENT_TIMESTAMP() capturado_em, CURRENT_TIMESTAMP() publicado_em, 5 geracao, 'pronto' estado
          FROM UNNEST(['supertabela','cras','creas']) servico;
        CREATE TEMP TABLE destino AS SELECT 'antigo' valor;
        CREATE TEMP TABLE candidato AS SELECT 'novo' valor;
        CREATE TEMP TABLE dim_publicada AS SELECT 'dim_antiga' valor;
        CREATE TEMP TABLE dim_candidata AS SELECT 'dim_nova' valor;
        """

    def transaction(self, generation):
        info = {
            "lote": "novo",
            "execucao": "nova",
            "capturado_em": "2026-10-26T00:00:00+00:00",
        }
        return module.transaction_sql(
            "controle",
            "destino",
            "candidato",
            [("dim_publicada", "dim_candidata")],
            generation,
            {"cras": info},
            info,
        )

    def test_service_move_requires_both_dimensions_with_real_sql(self):
        p = module.Pipeline.__new__(module.Pipeline)
        p.project = "p"
        fixtures = """WITH
          referencia AS (SELECT 1 ID_USUARIO,7 SEQ_ATENDIMENTO,'u' modulo),
          membros_usuarios AS (SELECT '7u' id_atendimento_modulo,10 id_unidade),
          membros_familias AS (SELECT '7f' id_atendimento_modulo,10 id_unidade FROM UNNEST([1]) WHERE FALSE),
          usuarios AS (SELECT 1 id_usuario),
          unidades AS (SELECT 10 id_unidade,'CREAS' tipo_unidade),
          evolucoes AS (SELECT CAST(NULL AS INT64) id_usuario,CAST(NULL AS INT64) id_paciente_familia,
            CAST(NULL AS INT64) id_unidade,CAST(NULL AS INT64) id_evolucao,
            CAST(NULL AS STRING) origem_modulo,CAST(NULL AS DATETIME) data_cancelamento FROM UNNEST([1]) WHERE FALSE),
          cras_anterior AS (SELECT 1 id_usuario,[STRUCT('7u' AS id_atendimento_modulo)] atendimentos,
            ARRAY<STRUCT<id_evolucao_modulo STRING>>[] evolucoes),
        """
        replacements = {
            "`p.base.atendimentos_programada`": "referencia",
            "`p.base.raw_atendimentos_usuarios`": "membros_usuarios",
            "`p.base.raw_atendimentos_familias`": "membros_familias",
            "`p.base.int_evolucoes`": "evolucoes",
            "`p.base.dim_usuarios`": "usuarios",
            "`p.base.dim_unidades`": "unidades",
            "`p.d.cras`": "cras_anterior",
        }

        def execute(sql):
            for source, fixture in replacements.items():
                sql = sql.replace(source, fixture)
            return list(self.client.query(fixtures + sql.removeprefix("WITH")).result())

        p.query = execute
        for selected in [["cras"], ["creas"]]:
            with self.assertRaisesRegex(ValueError, "juntas"):
                p.check_movements("base", {"cras": "p.d.cras"}, selected)
        p.check_movements("base", {"cras": "p.d.cras"}, ["cras", "creas"])
        p.check_movements("base", {"cras": "p.d.cras"}, ["centro_pop"])

    def test_success_changes_super_and_selected_dimension_together(self):
        result = list(
            self.client.query(
                self.setup_sql()
                + self.transaction(5)
                + """
          SELECT (SELECT valor FROM destino) valor, (SELECT valor FROM dim_publicada) dimensao,
            (SELECT geracao FROM controle WHERE servico='supertabela') geracao,
            (SELECT lote FROM controle WHERE servico='creas') preservado;
        """
            ).result()
        )[0]
        self.assertEqual(
            (result.valor, result.dimensao, result.geracao, result.preservado),
            ("novo", "dim_nova", 6, "anterior"),
        )

    def test_stale_generation_rolls_back_without_replacing_rows(self):
        sql = (
            self.setup_sql()
            + "BEGIN\n"
            + self.transaction(4)
            + """
        EXCEPTION WHEN ERROR THEN
          ROLLBACK TRANSACTION;
        END;
        SELECT (SELECT valor FROM destino) valor, (SELECT valor FROM dim_publicada) dimensao,
          (SELECT geracao FROM controle WHERE servico='supertabela') geracao;
        """
        )
        result = list(self.client.query(sql).result())[0]
        self.assertEqual(
            (result.valor, result.dimensao, result.geracao), ("antigo", "dim_antiga", 5)
        )


@unittest.skipUnless(
    os.getenv("RUN_BIGQUERY_TESTS") == "1", "BigQuery isolated copy/restore test"
)
class MigrationCopyTests(unittest.TestCase):
    def test_same_totals_cannot_hide_changed_keys_or_history(self):
        from google.api_core.exceptions import BadRequest

        client = module.bigquery.Client(
            project=os.getenv("BIGQUERY_TEST_PROJECT", "rj-smas-dev")
        )
        setup = """CREATE TEMP TABLE antes AS SELECT STRUCT('1' AS id_usuario) usuario,
            STRUCT(1 AS quantidade,[STRUCT('7u' AS id_atendimento_modulo)] AS registros) atendimentos,
            STRUCT(STRUCT(['original'] AS historico,['atividade'] AS participacoes) AS centro_pop) escopos;
        """
        for change in [
            "STRUCT(1 AS quantidade,[STRUCT('8u' AS id_atendimento_modulo)] AS registros) AS atendimentos",
            "STRUCT(STRUCT(['alterado'] AS historico,['atividade'] AS participacoes) AS centro_pop) AS escopos",
            "STRUCT('2' AS id_usuario) AS usuario",
        ]:
            with self.subTest(change=change):
                sql = (
                    setup
                    + "CREATE TEMP TABLE depois AS SELECT * REPLACE("
                    + change
                    + ") FROM antes;"
                )
                with self.assertRaisesRegex(BadRequest, "mudaram"):
                    list(
                        client.query(
                            sql + module.migration_parity_sql("antes", "depois")
                        ).result()
                    )

    def test_additive_projection_keeps_nested_order_values_and_null_structs(self):
        from google.api_core.exceptions import BadRequest

        client = module.bigquery.Client(
            project=os.getenv("BIGQUERY_TEST_PROJECT", "rj-smas-dev")
        )
        schema = [
            {
                "name": "usuario",
                "type": "RECORD",
                "fields": [
                    {"name": "id_usuario", "type": "STRING"},
                    {
                        "name": "contexto",
                        "type": "RECORD",
                        "fields": [{"name": "valor", "type": "STRING"}],
                    },
                ],
            },
            {
                "name": "itens",
                "type": "RECORD",
                "mode": "REPEATED",
                "fields": [{"name": "valor", "type": "STRING"}],
            },
        ]
        setup = """CREATE TEMP TABLE antes AS SELECT
            STRUCT('1' AS id_usuario, CAST(NULL AS STRUCT<valor STRING>) AS contexto) usuario,
            [STRUCT('a' AS valor),STRUCT('b' AS valor)] itens;
        """
        for change in ["same", "order", "null"]:
            context = (
                "STRUCT(CAST(NULL AS STRING) AS valor)"
                if change == "null"
                else "a.usuario.contexto"
            )
            direction = "DESC" if change == "order" else "ASC"
            candidate = f"""CREATE TEMP TABLE depois AS SELECT
                STRUCT(a.usuario.id_usuario AS id_usuario, {context} AS contexto, 'extra' AS novo) usuario,
                ARRAY(SELECT AS STRUCT i.*, 'extra' AS novo FROM UNNEST(a.itens) i WITH OFFSET pos ORDER BY pos {direction}) itens
                FROM antes a;"""

            def execute():
                return list(
                    client.query(
                        setup
                        + candidate
                        + module.migration_parity_sql("antes", "depois", schema)
                    ).result()
                )

            with self.subTest(change=change):
                if change == "same":
                    execute()
                else:
                    with self.assertRaisesRegex(BadRequest, "mudaram"):
                        execute()

    def test_schema_changing_failure_restores_real_copies(self):
        p = module.Pipeline.__new__(module.Pipeline)
        p.project = os.getenv("BIGQUERY_TEST_PROJECT", "rj-smas-dev")
        p.client = module.bigquery.Client(project=p.project, location="US")
        scratch = "pu_cand_qa_rollback_" + uuid4().hex
        dataset = module.bigquery.Dataset(f"{p.project}.{scratch}")
        dataset.location = "US"
        dataset.default_table_expiration_ms = 3600000
        p.client.create_dataset(dataset)

        def table(name):
            return f"{p.project}.{scratch}.{name}"

        p.core, p.control, p.destination = (
            scratch,
            table("controle"),
            table("supertabela"),
        )
        p.relation = lambda schema, name: table(
            name if name.startswith("_pu_") else "candidato_" + name
        )
        p.dataset, p.assert_paused = Mock(), Mock()
        p.base = Mock(return_value=("pu_base_lote", {}))
        p.validate_candidate_schema, p.check_derived_batch = Mock(), Mock()
        p.lock = Mock(return_value=nullcontext())
        p.write_json = Mock()
        try:
            p.query(f"""
                CREATE TABLE {module.quote(table("int_pu_registros_centro_pop"))} AS SELECT 1 id_usuario,'registro' id_registro;
                CREATE TABLE {module.quote(table("dim_centro_pop"))} AS SELECT 10 id_unidade,STRUCT('Unidade' AS nome) identificacao;
                CREATE TABLE {module.quote(p.destination)} AS {old_select_sql()};
                CREATE TABLE {module.quote(p.control)} AS SELECT 4 geracao,'supertabela' servico,'pronto' estado;
                CREATE TABLE {module.quote(table("candidato_int_pu_registros_centro_pop"))} AS SELECT * FROM {module.quote(table("int_pu_registros_centro_pop"))};
                CREATE TABLE {module.quote(table("candidato_dim_centro_pop"))} AS SELECT 1 id_usuario,'Sim' possui_registro_pai;
            """)
            schema_api = [
                f.to_api_repr() for f in p.client.get_table(p.destination).schema
            ]
            p.query(
                "CREATE TABLE "
                + module.quote(table("candidato_planilha_unificada"))
                + " AS SELECT "
                + module.simple_fields_projection(schema_api)
                + " FROM "
                + module.quote(p.destination)
                + " p"
            )
            old_schema = p.client.get_table(p.destination).schema
            baseline = table("_pu_antes_planilha_unificada")
            p.copy(p.destination, baseline)
            before = {
                t: p.fingerprint(t)
                for t in [
                    p.destination,
                    p.control,
                    table("dim_centro_pop"),
                    table("int_pu_registros_centro_pop"),
                ]
            }
            p.read_json = Mock(
                return_value={
                    "contrato": module.UNIFIED_CONTRACT,
                    "publicacao_permitida": True,
                    "geracao": 4,
                    "lote": "lote",
                    "tabela": p.fingerprint(table("candidato_planilha_unificada")),
                    "tabelas": {},
                    "antes_migracao": {
                        "tabela": baseline,
                        "fingerprint": p.fingerprint(baseline),
                        "publicadas": before,
                    },
                }
            )
            p.state = Mock(return_value={"supertabela": {"geracao": 4}})
            real_copy = p.copy
            count = 0

            def fail_after_new_schema(source, destination):
                nonlocal count
                count += 1
                real_copy(source, destination)
                if count == 3:
                    self.assertNotEqual(
                        p.client.get_table(destination).schema, old_schema
                    )
                    raise RuntimeError("Falha após trocar schema")

            p.copy = fail_after_new_schema
            with patch.object(module, "parity_sql", return_value="SELECT 1;"):
                with self.assertRaisesRegex(RuntimeError, "Falha após trocar schema"):
                    p.publish("teste")
            self.assertEqual(p.client.get_table(p.destination).schema, old_schema)
            self.assertEqual(p.write_json.call_args.args[2]["estado"], "restaurado")
            backups = p.write_json.call_args.args[2]["backups"]
            for destination, backup in backups.items():
                self.assertEqual(
                    p.client.get_table(destination).schema,
                    p.client.get_table(backup).schema,
                )
                p.query(module.table_equality_sql(backup, destination))
        finally:
            p.client.delete_dataset(
                dataset.reference, delete_contents=True, not_found_ok=True
            )


if __name__ == "__main__":
    unittest.main()
