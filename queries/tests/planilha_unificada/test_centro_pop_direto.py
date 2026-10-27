"""Strict relocation of the profile, keeping native values and four arrays."""

# ruff: noqa: S608 - SQL assembled exclusively from local synthetic fixtures

from copy import deepcopy
import os
import unittest

import test_complementos_operacao as helpers
import test_modelos as base

from queries.operations import planilha_unificada as op


def profile_schema():
    f = helpers.field
    result = [f("possui_registro_pai")]
    for subject, columns in base.cpop_profile_subjects().items():
        result.extend([f("data_registro_" + subject), f("data_resposta_" + subject)])
        for name in columns:
            public = (
                name + "_" + subject
                if name in {"demandas", "encaminhamentos", "observacoes"}
                else name
            )
            result.append(
                f(
                    public,
                    mode="REPEATED"
                    if name in {"demandas", "encaminhamentos"}
                    else "NULLABLE",
                )
            )
    return result


def schemas():
    f = helpers.field
    _, old = helpers.schemas()
    old.append(
        f(
            "escopos",
            "RECORD",
            fields=[
                f(
                    "centro_pop",
                    "RECORD",
                    fields=[
                        f("historico", "RECORD", "REPEATED", [f("tipo_registro")]),
                        f("participacoes", "RECORD", "REPEATED", [f("id_atividade")]),
                        f("respostas", "RECORD", fields=profile_schema()),
                    ],
                )
            ],
        )
    )
    new = deepcopy(old)
    centro = new[-1]["fields"][0]["fields"]
    wrapper = centro.pop()
    centro.extend(wrapper["fields"])
    return old, new


class DirectScopeSchemaTests(unittest.TestCase):
    def test_only_wrapper_is_removed_and_original_schema_is_not_mutated(self):
        old, new = schemas()
        original = deepcopy(old)
        op.validate_cpop_direct_schema(old, new, profile_schema())
        self.assertEqual(old, original)
        self.assertEqual(len(new[-1]["fields"][0]["fields"]), 37)
        self.assertEqual(
            sum(f["mode"] == "REPEATED" for f in new[-1]["fields"][0]["fields"][2:]), 4
        )
        op.validate_cpop_direct_schema(new, new, profile_schema())

    def test_missing_extra_changed_type_and_other_block_changes_are_rejected(self):
        old, new = schemas()
        variants = [old]
        missing = deepcopy(new)
        missing[-1]["fields"][0]["fields"].pop()
        variants.append(missing)
        extra = deepcopy(new)
        extra[-1]["fields"][0]["fields"].append(helpers.field("extra"))
        variants.append(extra)
        array_type = deepcopy(new)
        next(
            f
            for f in array_type[-1]["fields"][0]["fields"]
            if f["name"] == "demandas_pai"
        )["mode"] = "NULLABLE"
        variants.append(array_type)
        changed = deepcopy(new)
        changed[0]["fields"][0]["type"] = "INTEGER"
        variants.append(changed)
        for candidate in variants:
            with self.subTest(schema=candidate), self.assertRaises(ValueError):
                op.validate_cpop_direct_schema(old, candidate, profile_schema())

    def test_wrapped_source_must_match_the_dimension_profile(self):
        old, new = schemas()
        old[-1]["fields"][0]["fields"][2]["fields"][0]["name"] = "campo_desconhecido"
        with self.assertRaisesRegex(ValueError, "dimensão"):
            op.validate_cpop_direct_schema(old, new, profile_schema())

    def test_projection_redirects_only_the_profile_wrapper(self):
        old, _ = schemas()
        before = op.schema_projection(old)
        after = op.schema_projection(old, cpop_direct=True)
        self.assertIn("`centro_pop`.`respostas`.`possui_registro_pai`", before)
        self.assertNotIn("`centro_pop`.`respostas`", after)
        self.assertIn("`centro_pop`.`possui_registro_pai`", after)
        self.assertIn("AS `respostas`", after)
        self.assertIn("ORDER BY pos", after)
        self.assertIn("`endereco`.`logradouro`", after)


@unittest.skipUnless(
    os.getenv("RUN_BIGQUERY_TESTS") == "1", "Synthetic BigQuery relocation test"
)
class DirectScopeSqlTests(unittest.TestCase):
    def test_migration_preserves_profile_values_and_array_order(self):
        from google.api_core.exceptions import BadRequest
        from google.cloud import bigquery

        client = bigquery.Client(
            project=os.getenv("BIGQUERY_TEST_PROJECT", "rj-smas-dev")
        )
        f = helpers.field
        profile = profile_schema()
        schema = [f("usuario", "RECORD", fields=[f("id_usuario")]), schemas()[0][-1]]
        values = []
        for field in profile:
            if field["mode"] == "REPEATED":
                value = (
                    "['A','B']"
                    if field["name"] == "demandas_pai"
                    else "ARRAY<STRING>[]"
                )
            else:
                value = "'Sim'" if field["name"] == "possui_registro_pai" else "''"
            values.append(value + " AS `" + field["name"] + "`")
        setup = (
            """CREATE TEMP TABLE antes AS SELECT STRUCT('1' AS id_usuario) usuario,
          STRUCT(STRUCT([STRUCT('pai' AS tipo_registro)] AS historico,
          ARRAY<STRUCT<id_atividade STRING>>[] AS participacoes, STRUCT("""
            + ",".join(values)
            + ") AS respostas) AS centro_pop) escopos;"
        )
        for change in ["same", "scalar", "array_order", "history"]:
            targets = []
            for field in profile:
                value = "p.escopos.centro_pop.respostas.`" + field["name"] + "`"
                if change == "scalar" and field["name"] == "possui_registro_pai":
                    value = "'mudou'"
                if change == "array_order" and field["name"] == "demandas_pai":
                    value = "ARRAY_REVERSE(" + value + ")"
                targets.append(value + " AS `" + field["name"] + "`")
            history = (
                "[STRUCT('outro' AS tipo_registro)]"
                if change == "history"
                else "p.escopos.centro_pop.historico"
            )
            candidate = (
                "CREATE TEMP TABLE depois AS SELECT p.usuario, STRUCT((SELECT AS STRUCT "
                + history
                + " AS historico,p.escopos.centro_pop.participacoes AS participacoes,"
                + ",".join(targets)
                + ") AS centro_pop) escopos FROM antes p;"
            )
            with self.subTest(change=change):
                query = (
                    setup
                    + candidate
                    + op.migration_parity_sql(
                        "antes", "depois", schema, cpop_direct=True
                    )
                )
                if change == "same":
                    list(client.query(query).result())
                else:
                    with self.assertRaisesRegex(BadRequest, "mudaram"):
                        list(client.query(query).result())
