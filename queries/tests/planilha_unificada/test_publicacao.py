# ruff: noqa: S608 - queries assembled exclusively from local synthetic fixtures
"""Publication failure paths; no credentials needed unless RUN_BIGQUERY_TESTS=1."""

from contextlib import nullcontext
from datetime import datetime, timezone
import importlib.util
import json
import os
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import Mock

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
        return p

    def test_parse_registers_dimension_dependencies_before_execution(self):
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
            (module.ROOT / "macros/planilha_unificada/dimensoes.sql").read_text()
        ).module
        macros.pu_ref("int_pu_atendimentos")
        self.assertTrue(set(module.SERVICES.values()).issubset(references))

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
        p.context = Mock(return_value=("pu_cand_teste", {}, {}))
        p.read_json = Mock(side_effect=module.NotFound("not built"))
        p.dbt.side_effect = RuntimeError("test failed")
        with self.assertRaises(RuntimeError):
            p.build_dimension("lote", "teste", "cras")
        p.write_json.assert_not_called()

    def test_initial_consolidation_requires_every_service(self):
        p = self.pipeline()
        p.context = Mock(return_value=("pu_cand_teste", {}, {}))
        p.state = Mock(return_value={})
        with self.assertRaisesRegex(ValueError, "quatro"):
            p.consolidate("lote", "teste", ["cras"])
        p.dbt.assert_not_called()

    def test_older_dimension_is_rejected_before_consolidation(self):
        p = self.pipeline()
        p.context = Mock(return_value=("pu_cand_teste", {}, {}))
        p.state = Mock(
            return_value={
                "supertabela": {"geracao": 1, "estado": "pronto"},
                "cras": {"capturado_em": datetime(2026, 10, 26, tzinfo=timezone.utc)},
            }
        )
        p.read_json = Mock(
            return_value={
                "capturado_em": "2026-10-25T00:00:00+00:00",
                "tabela": {},
                "validacao": "programada_v2",
            }
        )
        with self.assertRaisesRegex(ValueError, "antigo"):
            p.consolidate("lote", "teste", list(module.SERVICES))
        p.dbt.assert_not_called()

    def test_stale_publication_does_not_mutate_any_table(self):
        p = self.pipeline()
        p.read_json = Mock(
            return_value={
                "tabela": {},
                "tabelas": {},
                "lote": "lote",
                "cadastro": {"lote": "lote"},
                "geracao": 3,
                "referencia": {"versao": "v"},
                "servicos": list(module.SERVICES),
            }
        )
        p.base = Mock()
        p.state = Mock(
            return_value={
                "supertabela": {"geracao": 4, "execucao": "outra", "estado": "pronto"}
            }
        )
        with self.assertRaisesRegex(ValueError, "Geração"):
            p.publish("teste")
        p.query.assert_not_called()

    def test_changed_candidate_is_not_published(self):
        p = self.pipeline()
        p.read_json = Mock(
            return_value={
                "tabela": {},
                "referencia": {"versao": "v"},
                "servicos": list(module.SERVICES),
            }
        )
        p.verify.side_effect = ValueError("Tabela mudou")
        with self.assertRaisesRegex(ValueError, "mudou"):
            p.publish("teste")
        p.query.assert_not_called()
        p.initialize_control.assert_not_called()

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
          fct AS (SELECT 1 id_usuario,'7u' id_atendimento_modulo,10 id_unidade,'N' flag_cancelado),
          usuarios AS (SELECT 1 id_usuario),
          unidades AS (SELECT 10 id_unidade,'CREAS' tipo_unidade),
          evolucoes AS (SELECT CAST(NULL AS INT64) id_usuario,CAST(NULL AS INT64) id_paciente_familia,
            CAST(NULL AS INT64) id_unidade,CAST(NULL AS INT64) id_evolucao,
            CAST(NULL AS STRING) origem_modulo,CAST(NULL AS DATETIME) data_cancelamento FROM UNNEST([1]) WHERE FALSE),
          cras_anterior AS (SELECT 1 id_usuario,[STRUCT('7u' AS id_atendimento_modulo)] atendimentos,
            ARRAY<STRUCT<id_evolucao_modulo STRING>>[] evolucoes),
        """
        replacements = {
            "`p.base.fct_atendimentos`": "fct",
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


if __name__ == "__main__":
    unittest.main()
