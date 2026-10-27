"""V9 migration: full-row parity, repeated answers and strict schema changes."""

# ruff: noqa: S608 - local synthetic SQL fixtures only
from copy import deepcopy
import os
import unittest

from test_complementos_operacao import field as f

from queries.operations import planilha_unificada as op
from queries.operations.referencia_programada import parity_sql


def old_schema():
    user = [f("id_usuario"), f("nome")]
    for path, names in op.SIMPLE_GROUPS.items():
        if path.startswith("usuario."):
            user.append(f(path.split(".")[1], "RECORD", fields=[f(n) for n in names]))
    answer = f("campos", "RECORD", "REPEATED", [f("label"), f("valor")])
    form = f(
        "formularios",
        "RECORD",
        "REPEATED",
        [f("titulo_formulario"), f("observacoes"), answer],
    )
    result = [f("usuario", "RECORD", fields=user)]
    for block in ["atendimentos", "evolucoes"]:
        records = [
            f(
                "id_atendimento_modulo"
                if block == "atendimentos"
                else "id_evolucao_modulo"
            )
        ]
        if block == "evolucoes":
            records.append(form)
        result.append(
            f(
                block,
                "RECORD",
                fields=[
                    f("quantidade", "INTEGER"),
                    f(
                        "quantidade_por_servico",
                        "RECORD",
                        fields=[
                            f(n, "INTEGER")
                            for n in ["cras", "creas", "centro_pop", "outros"]
                        ],
                    ),
                    f("registros", "RECORD", "REPEATED", records),
                ],
            )
        )
    result.append(
        f(
            "escopos",
            "RECORD",
            fields=[
                f(
                    "centro_pop",
                    "RECORD",
                    fields=[
                        f("historico", mode="REPEATED"),
                        f("participacoes", mode="REPEATED"),
                    ],
                )
            ],
        )
    )
    return result


def old_select_sql():
    groups = []
    for path, names in op.SIMPLE_GROUPS.items():
        if path.startswith("usuario."):
            groups.append(
                "STRUCT("
                + ",".join("'' AS " + n for n in names)
                + ") AS "
                + path.split(".")[1]
            )
    user = (
        "STRUCT('1' AS id_usuario,'Pessoa' AS nome," + ",".join(groups) + ") AS usuario"
    )
    counts = "STRUCT(1 AS cras,0 AS creas,0 AS centro_pop,0 AS outros) AS quantidade_por_servico"
    return (
        "SELECT "
        + user
        + ",STRUCT(1 AS quantidade,"
        + counts
        + ",[STRUCT('7u' AS id_atendimento_modulo)] AS registros) atendimentos,STRUCT(1 AS quantidade,"
        + counts
        + """,
      [STRUCT('8u' AS id_evolucao_modulo,[
        STRUCT('Título' AS titulo_formulario,'Observação' AS observacoes,
          [STRUCT('Nova pergunta' AS label,'A: B, C & D' AS valor),STRUCT('Repete' AS label,'1' AS valor),STRUCT('Repete' AS label,'2' AS valor),STRUCT('Repete' AS label,'2' AS valor),STRUCT('' AS label,'' AS valor)] AS campos),
        STRUCT('Vazio' AS titulo_formulario,'' AS observacoes,ARRAY<STRUCT<label STRING,valor STRING>>[] AS campos)
      ] AS formularios)] AS registros) evolucoes,
      STRUCT(STRUCT(['histórico'] AS historico,ARRAY<STRING>[] AS participacoes) AS centro_pop) escopos"""
    )


class SimpleSchemaTests(unittest.TestCase):
    def test_schema_moves_only_approved_groups_and_does_not_mutate_source(self):
        old = old_schema()
        before = deepcopy(old)
        new = op.simple_fields_schema(old)
        op.validate_simple_fields_schema(old, new)
        self.assertEqual(old, before)
        self.assertEqual(new[-1], old[-1])
        self.assertTrue(any(v["name"] == "logradouro" for v in new[0]["fields"]))
        self.assertIn("ORDER BY", op.simple_fields_projection(old))

    def test_current_contract_can_be_reconsolidated_without_transforming_answers_again(
        self,
    ):
        new = op.simple_fields_schema(old_schema())
        self.assertEqual(op.simple_fields_schema(new), new)
        op.validate_simple_fields_schema(new, new)
        self.assertNotIn("c.label", op.simple_fields_projection(new))
        broken = deepcopy(new)
        broken[0]["fields"].pop()
        with self.assertRaises(ValueError):
            op.simple_fields_schema(broken)

    def test_reference_parity_accepts_both_counter_contracts(self):
        old = parity_sql("antes", "depois", op.quote)
        new = parity_sql("antes", "depois", op.quote, direct_counts=True)
        self.assertIn("atendimentos.quantidade_por_servico.cras", old)
        self.assertNotIn("quantidade_por_servico", new)
        self.assertIn("atendimentos.quantidade_cras", new)

    def test_missing_extra_wrong_type_order_and_other_changes_fail(self):
        old = old_schema()
        new = op.simple_fields_schema(old)
        for change in ["missing", "extra", "type", "order", "scope"]:
            candidate = deepcopy(new)
            if change == "missing":
                candidate[0]["fields"].pop()
            if change == "extra":
                candidate[0]["fields"].append(f("extra"))
            if change == "type":
                candidate[1]["fields"][1]["type"] = "STRING"
            if change == "order":
                candidate[0]["fields"].reverse()
            if change == "scope":
                candidate[-1]["fields"][0]["fields"].pop()
            with self.subTest(change=change), self.assertRaises(ValueError):
                op.validate_simple_fields_schema(old, candidate)

    def test_missing_source_group_and_colliding_names_are_rejected(self):
        for change in ["missing", "collision", "wrong_answer"]:
            old = old_schema()
            if change == "missing":
                old[0]["fields"].pop()
            if change == "collision":
                old[0]["fields"].append(f("logradouro"))
            if change == "wrong_answer":
                old[2]["fields"][2]["fields"][1]["fields"][2]["fields"][1]["type"] = (
                    "INTEGER"
                )
            with self.subTest(change=change), self.assertRaises(ValueError):
                op.simple_fields_schema(old)


@unittest.skipUnless(
    os.getenv("RUN_BIGQUERY_TESTS") == "1", "Synthetic BigQuery v9 migration"
)
class SimpleSqlTests(unittest.TestCase):
    def test_reference_parity_with_direct_counters_detects_a_changed_count(self):
        from google.api_core.exceptions import BadRequest
        from google.cloud import bigquery

        client = bigquery.Client(project="rj-smas-dev")
        setup = "CREATE TEMP TABLE referencia AS SELECT 7 SEQ_ATENDIMENTO,'u' modulo,1 ID_USUARIO,DATE '2026-01-01' DATA_DE_ATENDIMENTO,'0900' HORA_DE_ATENDIMENTO,'CRAS' UNIDADE_ATENDIMENTO,'Atendimento' NOME_ATENDIMENTO_ORIGINAL;"
        setup += "CREATE TEMP TABLE antes AS " + old_select_sql() + ";"
        setup += (
            "CREATE TEMP TABLE depois AS SELECT "
            + op.simple_fields_projection(old_schema())
            + " FROM antes p;"
        )
        check = parity_sql("referencia", "depois", op.quote, direct_counts=True)
        list(client.query(setup + check).result())
        with self.assertRaisesRegex(BadRequest, "Contagem não corresponde"):
            list(
                client.query(
                    setup
                    + "UPDATE depois SET atendimentos.quantidade_cras=0 WHERE TRUE;"
                    + check
                ).result()
            )

    def test_full_row_comparison_detects_value_order_multiplicity_and_key_changes(self):
        from google.api_core.exceptions import BadRequest
        from google.cloud import bigquery

        client = bigquery.Client(project="rj-smas-dev")
        projection = op.simple_fields_projection(old_schema())
        setup = "CREATE TEMP TABLE antes AS " + old_select_sql() + ";"
        changes = {
            "same": "",
            "scalar": "UPDATE depois SET usuario.nome='Mudou' WHERE TRUE;",
            "key": "UPDATE depois SET usuario.id_usuario='2' WHERE TRUE;",
            "scope": "UPDATE depois SET escopos.centro_pop.historico=['alterado'] WHERE TRUE;",
            "order": "CREATE OR REPLACE TEMP TABLE depois AS SELECT p.* REPLACE((SELECT AS STRUCT p.evolucoes.* EXCEPT(registros),ARRAY(SELECT AS STRUCT e.* REPLACE(ARRAY(SELECT AS STRUCT f.* REPLACE(ARRAY_REVERSE(f.campos) AS campos) FROM UNNEST(e.formularios) f WITH OFFSET pos ORDER BY pos) AS formularios) FROM UNNEST(p.evolucoes.registros) e WITH OFFSET pos ORDER BY pos) AS registros) AS evolucoes) FROM depois p;",
            "dedup": "CREATE OR REPLACE TEMP TABLE depois AS SELECT p.* REPLACE((SELECT AS STRUCT p.evolucoes.* EXCEPT(registros),ARRAY(SELECT AS STRUCT e.* REPLACE(ARRAY(SELECT AS STRUCT f.* REPLACE(ARRAY(SELECT DISTINCT x FROM UNNEST(f.campos) x) AS campos) FROM UNNEST(e.formularios) f WITH OFFSET pos ORDER BY pos) AS formularios) FROM UNNEST(p.evolucoes.registros) e WITH OFFSET pos ORDER BY pos) AS registros) AS evolucoes) FROM depois p;",
        }
        for change, mutation in changes.items():
            sql = (
                setup
                + "CREATE TEMP TABLE depois AS SELECT "
                + projection
                + " FROM antes p;"
                + mutation
                + op.migration_parity_sql(
                    "antes", "depois", old_schema(), simple_fields=True
                )
            )
            with self.subTest(change=change):
                if change == "same":
                    sql += op.migration_parity_sql(
                        "depois",
                        "depois",
                        op.simple_fields_schema(old_schema()),
                        simple_fields=True,
                    )
                    rows = list(
                        client.query(
                            sql
                            + "SELECT evolucoes.registros[OFFSET(0)].formularios AS formularios FROM depois;"
                        ).result()
                    )
                    self.assertEqual(
                        rows[0].formularios[0]["campos"],
                        [
                            "Nova pergunta: A: B, C & D",
                            "Repete: 1",
                            "Repete: 2",
                            "Repete: 2",
                            ": ",
                        ],
                    )
                    self.assertEqual(rows[0].formularios[1]["campos"], [])
                else:
                    with self.assertRaisesRegex(BadRequest, "mudaram"):
                        list(client.query(sql).result())
