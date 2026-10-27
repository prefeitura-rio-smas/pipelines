"""Schema, inherited input and publication contract checks without credentials."""

from contextlib import nullcontext
from copy import deepcopy
import unittest
from unittest.mock import Mock

from queries.operations import planilha_unificada as op


def field(name, kind="STRING", mode="NULLABLE", fields=None):
    result = dict(name=name, type=kind, mode=mode)
    if fields is not None:
        result["fields"] = fields
    return result


def schemas():
    original = [
        field("usuario", "RECORD", fields=[field("id_usuario")]),
        field(
            "atendimentos",
            "RECORD",
            fields=[
                field("quantidade", "INTEGER"),
                field(
                    "registros",
                    "RECORD",
                    "REPEATED",
                    [
                        field("id_atendimento_modulo"),
                        field(
                            "profissionais",
                            "RECORD",
                            "REPEATED",
                            [field("nome_profissional")],
                        ),
                    ],
                ),
            ],
        ),
    ]
    new = deepcopy(original)
    new[0]["fields"].append(
        field(
            "endereco",
            "RECORD",
            fields=[
                field(n)
                for n in [
                    "logradouro",
                    "numero",
                    "complemento",
                    "referencia_ou_comunidade",
                ]
            ],
        )
    )
    events = new[1]["fields"][1]["fields"]
    events.extend(
        field(n)
        for n in [
            "cas",
            "email_unidade",
            "email_cas",
            "email",
            "cadastrante",
            "data_cadastro_atendimento",
        ]
    )
    events[1]["fields"].extend(
        field(n)
        for n in ["id_profissional", "id_familia_referencia", "id_atendimento_legado"]
    )
    return original, new


class BiSchemaTests(unittest.TestCase):
    def test_only_thirteen_approved_leaves_are_accepted(self):
        old, new = schemas()
        op.validate_bi_schema(old, new)
        op.validate_bi_schema(new, new)

    def test_extra_field_missing_field_changed_type_and_reordered_fields_fail(self):
        old, new = schemas()
        variants = []
        extra = deepcopy(new)
        extra[0]["fields"].append(field("segredo"))
        variants.append(extra)
        missing = deepcopy(new)
        missing[0]["fields"][1]["fields"].pop()
        variants.append(missing)
        changed = deepcopy(new)
        changed[1]["fields"][0]["type"] = "STRING"
        variants.append(changed)
        wrong_id_type = deepcopy(new)
        wrong_id_type[1]["fields"][1]["fields"][1]["fields"][1]["type"] = "INTEGER"
        variants.append(wrong_id_type)
        reordered = list(reversed(new))
        variants.append(reordered)
        for schema in variants:
            with self.subTest(schema=schema), self.assertRaises(ValueError):
                op.validate_bi_schema(old, schema)

    def test_projection_preserves_array_order_and_null_structs(self):
        old, _ = schemas()
        sql = op.schema_projection(old)
        self.assertIn("ORDER BY pos1", sql)
        self.assertIn("ORDER BY pos2", sql)
        self.assertIn("IF(p.`usuario` IS NULL, NULL", sql)
        self.assertNotIn("endereco", sql)
        self.assertNotIn("id_atendimento_legado", sql)
        with self.assertRaises(ValueError):
            op.schema_projection([field("bad`name")])


class DerivedBatchTests(unittest.TestCase):
    def setup_pipeline(self):
        p = op.Pipeline.__new__(op.Pipeline)
        p.project, p.target = "rj-smas-dev", "staging"
        parent = dict(
            fontes={
                "usuarios": dict(rows=1, schema=[field("id_usuario")], etag="parent")
            },
            referencia={"versao": "v"},
            capturado_em="original",
        )
        child = dict(
            parent,
            lote_pai="parent",
            lote_pai_manifesto={"etag": "m"},
            herdadas=deepcopy(parent["fontes"]),
            fontes=deepcopy(parent["fontes"]),
        )
        child["fontes"]["usuarios"]["etag"] = "copy"
        p.base = Mock(return_value=("pu_base_parent", parent))
        p.verify = Mock()
        return p, parent, child

    def test_derived_copy_preserves_parent_identity_and_reference(self):
        p, _, child = self.setup_pipeline()
        p.check_derived_batch(child)
        p.verify.assert_called_once_with(
            "rj-smas-dev.pu_base_parent._pu_lote", {"etag": "m"}
        )

    def test_modified_parent_reference_or_copy_metadata_are_rejected(self):
        for key in ["reference", "inherited", "copy", "timestamp"]:
            p, _, child = self.setup_pipeline()
            if key == "reference":
                child["referencia"] = {"versao": "changed"}
            if key == "inherited":
                child["herdadas"] = {}
            if key == "copy":
                child["fontes"]["usuarios"]["rows"] = 2
            if key == "timestamp":
                child["capturado_em"] = "changed"
            with self.subTest(key=key), self.assertRaises(ValueError):
                p.check_derived_batch(child)

    def test_existing_address_snapshot_is_inherited_without_recapture(self):
        p, parent, _ = self.setup_pipeline()
        parent["fontes"]["raw_usuarios_enderecos"] = deepcopy(
            parent["fontes"]["usuarios"]
        )
        parent["complementos"] = {
            "raw_usuarios_enderecos": {"capturado_em": "snapshot_original"}
        }
        p.dataset, p.copy, p.write_json, p.dbt = Mock(), Mock(), Mock(), Mock()
        p.read_json = Mock(side_effect=op.NotFound("not sealed"))
        p.lock = Mock(return_value=nullcontext())
        p.fingerprint = Mock(return_value={"etag": "clone"})
        p.derive_batch("child", "parent")
        p.dbt.assert_not_called()
        self.assertEqual(p.copy.call_count, 2)
        result = p.write_json.call_args.args[2]
        self.assertEqual(result["complementos"], parent["complementos"])
        self.assertEqual(result["herdadas"], parent["fontes"])

    def test_cleanup_keeps_parent_of_a_referenced_derived_batch(self):
        p, _, _ = self.setup_pipeline()
        p.state = Mock(
            return_value={"supertabela": dict(lote="child", execucao="current")}
        )
        p.client = Mock()
        p.client.list_datasets.return_value = []
        p.dataset = Mock()
        p.read_json = Mock(
            side_effect=lambda schema, name: (
                {"lote_pai": "parent"} if schema == "pu_base_child" else {}
            )
        )
        with self.assertRaisesRegex(ValueError, "referenciado"):
            p.clean(["pu_base_parent"])
        p.client.delete_dataset.assert_not_called()
