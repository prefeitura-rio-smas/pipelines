"""Build frozen user data with direct fields and readable form answers.

Use preparar, dimensao --servico centro_pop and consolidar --servicos centro_pop.
Use publicar for the manual migration with backups and compensating restoration.
The scheduled query remains paused; automatic cycles stay disabled.
"""

# SQL templates use quote()/literal() for validated relations and scalar literals; JSON payloads use parameters.
# ruff: noqa: S608
from __future__ import annotations

import argparse
from contextlib import contextmanager
from copy import deepcopy
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import subprocess
import sys

from google.api_core.exceptions import Conflict, NotFound
from google.cloud import bigquery
import yaml

try:
    from queries.operations.referencia_programada import (
        CANDIDATE,
        PUBLIC,
        Producer,
        parity_sql,
        reference_membership_sql,
        version,
    )
except ModuleNotFoundError:  # CLI invoked with queries/operations as sys.path[0]
    from referencia_programada import (
        CANDIDATE,
        PUBLIC,
        Producer,
        parity_sql,
        reference_membership_sql,
        version,
    )

ROOT = Path(__file__).resolve().parents[1]
SCOPE_CONTRACT = "centro_pop_usuario_v3"
UNIFIED_CONTRACT = "por_assunto_v9_campos_diretos_formularios_texto"
BI_FIELDS = {
    **dict.fromkeys(
        (
            "usuario.endereco." + f
            for f in ["logradouro", "numero", "complemento", "referencia_ou_comunidade"]
        ),
        "STRING",
    ),
    **dict.fromkeys(
        (
            "atendimentos.registros." + f
            for f in [
                "cas",
                "email_unidade",
                "email_cas",
                "email",
                "cadastrante",
                "data_cadastro_atendimento",
            ]
        ),
        "STRING",
    ),
    **dict.fromkeys(
        (
            "atendimentos.registros.profissionais." + f
            for f in [
                "id_profissional",
                "id_familia_referencia",
                "id_atendimento_legado",
            ]
        ),
        "STRING",
    ),
}
SERVICES = {
    "cras": "dim_cras",
    "creas": "dim_creas",
    "centro_pop": "dim_centro_pop",
    "outros": "dim_outros_equipamentos",
}


def identifier(value: str, limit: int = 80) -> str:
    if not re.fullmatch(r"[a-z][a-z0-9_]*", value) or len(value) > limit:
        raise ValueError(
            f"Use identificadores com letra inicial, minúsculas, números e _, até {limit} caracteres."
        )
    return value


def quote(value: str) -> str:
    # Relations are generated from validated identifiers or the dbt manifest.
    if not re.fullmatch(r"[a-zA-Z0-9_.:\-]+", value):
        raise ValueError(f"Relação inválida: {value!r}")
    return f"`{value}`"


def literal(value) -> str:
    return "'" + str(value).replace("\\", "\\\\").replace("'", "\\'") + "'"


def stamp(value) -> str:
    return value.isoformat() if isinstance(value, datetime) else value


def transaction_sql(
    control,
    destination,
    candidate,
    dimensions,
    generation,
    updates,
    cadastro,
    reference=None,
    programada=None,
    reference_version=None,
    checks="",
    dimension_manifest=None,
):
    """One atomic DML transaction; a stale candidate never overwrites a newer run."""
    parts = [
        "BEGIN TRANSACTION;",
        f"ASSERT (SELECT COUNT(*) = 1 FROM {quote(control)} WHERE servico = 'supertabela' "
        f"AND geracao = {int(generation)} AND estado = 'pronto') AS 'Geração mudou; reconsolide';",
        # Updating the same row also makes concurrent transactions conflict in BigQuery.
        f"UPDATE {quote(control)} SET geracao = geracao + 1 WHERE servico = 'supertabela';",
    ]
    for published, source in dimensions:
        parts += [
            f"DELETE FROM {quote(published)} WHERE TRUE;",
            f"INSERT INTO {quote(published)} SELECT * FROM {quote(source)};",
        ]
    parts += [
        f"DELETE FROM {quote(destination)} WHERE TRUE;",
        f"INSERT INTO {quote(destination)} SELECT * FROM {quote(candidate)};",
    ]
    for service, info in {**updates, "supertabela": cadastro}.items():
        parts.append(
            f"UPDATE {quote(control)} SET lote={literal(info['lote'])}, execucao={literal(info['execucao'])}, "
            f"capturado_em=TIMESTAMP({literal(info['capturado_em'])}), publicado_em=CURRENT_TIMESTAMP(), "
            f"geracao={int(generation) + 1}, estado='pronto' WHERE servico={literal(service)};"
        )
    if reference is not None:
        parts += [
            f"DELETE FROM {quote(programada)} WHERE TRUE;",
            f"INSERT INTO {quote(programada)} SELECT * FROM {quote(reference)};",
        ]
        parts.append(parity_sql(programada, destination, quote))
        parts.append(
            f"UPDATE {quote(control)} SET referencia_versao={literal(reference_version)} WHERE servico IN ('supertabela','programada','cras','creas','centro_pop','outros');"
        )
    if dimension_manifest is not None:
        for service in updates:
            parts.append(
                f"UPDATE {quote(control)} SET referencia_versao={literal(dimension_manifest[service]['referencia_versao'])} WHERE servico={literal(service)};"
            )
        versions = {v["referencia_versao"] for v in dimension_manifest.values()}
        common = literal(next(iter(versions))) if len(versions) == 1 else "NULL"
        payload = literal(json.dumps(dimension_manifest, default=stamp, sort_keys=True))
        parts.append(
            f"UPDATE {quote(control)} SET dimensoes_utilizadas=PARSE_JSON({payload}),referencia_versao={common} WHERE servico='supertabela';"
        )
    parts += [checks, "COMMIT TRANSACTION;"]
    return "\n".join(parts)


def table_equality_sql(left, right):
    """Exact row multiset comparison, including duplicates and nested array order."""
    return f"""ASSERT NOT EXISTS (
      WITH registros AS (
        SELECT TO_JSON_STRING(r) valor, 1 lado FROM {quote(left)} r
        UNION ALL SELECT TO_JSON_STRING(r), -1 FROM {quote(right)} r)
      SELECT valor FROM registros GROUP BY valor HAVING SUM(lado) != 0
    ) AS 'Dados divergentes após materialização/restauração';"""


def schema_projection(schema, alias="p", level=0, cpop_direct=False, prefix=""):
    """Project nested arrays onto the old schema, preserving order and null STRUCTs."""
    output = []
    for field in schema:
        name = field["name"]
        if not re.fullmatch(r"[a-zA-Z_][a-zA-Z0-9_]*", name):
            raise ValueError("Nome de coluna inválido no schema")
        logical_path = prefix + name
        path = (
            alias
            if cpop_direct and logical_path == "escopos.centro_pop.respostas"
            else f"{alias}.`{name}`"
        )
        if field.get("fields"):
            if field.get("mode") == "REPEATED":
                item, offset = f"item{level}", f"pos{level}"
                nested = schema_projection(
                    field["fields"], item, level + 1, cpop_direct, logical_path + "."
                )
                expr = f"ARRAY(SELECT AS STRUCT {nested} FROM UNNEST({path}) AS {item} WITH OFFSET AS {offset} ORDER BY {offset})"
            else:
                nested = schema_projection(
                    field["fields"], path, level + 1, cpop_direct, logical_path + "."
                )
                expr = f"IF({path} IS NULL, NULL, STRUCT({nested}))"
        else:
            expr = path
        output.append(f"{expr} AS `{name}`")
    return ", ".join(output)


def schema_signature(fields):
    return tuple(
        (
            f["name"],
            f["type"],
            f.get("mode", "NULLABLE"),
            schema_signature(f.get("fields", [])),
        )
        for f in fields
    )


def cpop_direct_schema(schema, profile):
    """Move only the approved 35 profile fields; preserve every other schema node."""
    if len(profile) != 35 or len({f["name"] for f in profile}) != 35:
        raise ValueError("Perfil Centro POP deve conter 35 campos distintos")
    if sum(f.get("mode", "NULLABLE") == "REPEATED" for f in profile) != 4 or any(
        f["type"] != "STRING"
        or f.get("fields")
        or f.get("mode", "NULLABLE") not in {"NULLABLE", "REPEATED"}
        for f in profile
    ):
        raise ValueError("Perfil exige 31 STRING e quatro ARRAY<STRING>")
    result = deepcopy(schema)
    try:
        scopes = next(f for f in result if f["name"] == "escopos")
        centro = next(f for f in scopes["fields"] if f["name"] == "centro_pop")
    except (StopIteration, KeyError) as error:
        raise ValueError("Schema sem escopos.centro_pop") from error
    if (
        scopes["type"] not in {"RECORD", "STRUCT"}
        or centro["type"] not in {"RECORD", "STRUCT"}
        or centro.get("mode", "NULLABLE") != "NULLABLE"
    ):
        raise ValueError("Tipo do escopo Centro POP incompatível")
    children = centro["fields"]
    if [f["name"] for f in children[:2]] != ["historico", "participacoes"]:
        raise ValueError(
            "Histórico e participações devem permanecer nas posições anteriores"
        )
    if any(f["name"] == "respostas" for f in children):
        if (
            len(children) != 3
            or children[2]["name"] != "respostas"
            or children[2]["type"] not in {"RECORD", "STRUCT"}
            or children[2].get("mode", "NULLABLE") != "NULLABLE"
        ):
            raise ValueError("Agrupador respostas incompatível com a migração")
        if schema_signature(children[2].get("fields", [])) != schema_signature(profile):
            raise ValueError("Perfil anterior diverge da dimensão Centro POP")
        centro["fields"] = children[:2] + deepcopy(profile)
    elif schema_signature(children[2:]) != schema_signature(profile):
        raise ValueError("Campos diretos do perfil divergem da dimensão Centro POP")
    return result


def validate_cpop_direct_schema(old, new, profile):
    expected = cpop_direct_schema(old, profile)
    # Also assert the candidate exposes the direct shape, never the old wrapper.
    direct = cpop_direct_schema(new, profile)
    if schema_signature(direct) != schema_signature(new) or schema_signature(
        expected
    ) != schema_signature(new):
        raise ValueError(
            "Schema permite somente mover respostas para escopos.centro_pop"
        )


def validate_bi_schema(old, new):
    """Only the thirteen approved public leaves may extend the existing contract."""
    found = set()

    def walk(left, right, prefix=""):
        if len(right) < len(left):
            raise ValueError("Schema remove campos anteriores")
        for index, field in enumerate(right):
            name = field["name"]
            path = prefix + name
            previous = left[index] if index < len(left) else None
            if previous is not None:

                def signature(f):
                    return (f["name"], f["type"], f.get("mode", "NULLABLE"))

                if signature(previous) != signature(field):
                    raise ValueError("Schema altera campo anterior: " + path)
            if field.get("fields"):
                if previous is None and not (
                    field["type"] in {"RECORD", "STRUCT"}
                    and field.get("mode", "NULLABLE") == "NULLABLE"
                    and any(p.startswith(path + ".") for p in BI_FIELDS)
                ):
                    raise ValueError("STRUCT adicional não autorizado: " + path)
                walk(
                    previous.get("fields", []) if previous else [],
                    field["fields"],
                    path + ".",
                )
            elif path in BI_FIELDS:
                if (
                    field["type"] != BI_FIELDS[path]
                    or field.get("mode", "NULLABLE") != "NULLABLE"
                ):
                    raise ValueError("Tipo incorreto no complemento: " + path)
                found.add(path)
            elif previous is None:
                raise ValueError("Campo adicional não autorizado: " + path)

    walk(old, new)
    if found != set(BI_FIELDS):
        raise ValueError("Schema incompleto dos treze complementos BI")


SIMPLE_GROUPS = {
    "usuario.cadastro_complementar": (
        "uf_nascimento",
        "serie_escolar",
        "data_cadunico_informada",
    ),
    "usuario.documentos_cadunico": (
        "nis",
        "numero_rg",
        "possui_rg",
        "possui_certidao_nascimento",
        "numero_certidao_nascimento",
        "data_atualizacao",
        "data_limite_cadastro_atual",
        "flag_familia_ambigua",
        "flag_nis_ambiguo",
        "flag_rg_ambiguo",
        "flag_certidao_ambigua",
    ),
    "usuario.endereco": (
        "logradouro",
        "numero",
        "complemento",
        "referencia_ou_comunidade",
    ),
    "atendimentos.quantidade_por_servico": ("cras", "creas", "centro_pop", "outros"),
    "evolucoes.quantidade_por_servico": ("cras", "creas", "centro_pop", "outros"),
}
FORM_FIELDS_PATH = "evolucoes.registros.formularios.campos"


def simple_fields_schema(schema):
    """Only the five approved groups and form answer representation may change."""
    # Subsequent manual consolidations must also accept the complete v9 shape.
    leaves = {}

    def inventory(fields, prefix=""):
        for field in fields:
            path = prefix + field["name"]
            leaves[path] = field
            inventory(field.get("fields", []), path + ".")

    inventory(schema)
    if not any(path in leaves for path in SIMPLE_GROUPS):
        for group, names in SIMPLE_GROUPS.items():
            parent = group.rsplit(".", 1)[0]
            numeric = group.endswith("quantidade_por_servico")
            for name in names:
                path = parent + "." + ("quantidade_" if numeric else "") + name
                field = leaves.get(path, {})
                if (
                    field.get("type")
                    not in ({"INTEGER", "INT64"} if numeric else {"STRING"})
                    or field.get("mode", "NULLABLE") != "NULLABLE"
                    or field.get("fields")
                ):
                    raise ValueError("Schema v9 incompleto: " + path)
        answers = leaves.get(FORM_FIELDS_PATH, {})
        if (
            answers.get("type") != "STRING"
            or answers.get("mode") != "REPEATED"
            or answers.get("fields")
        ):
            raise ValueError("Respostas v9 exigem ARRAY<STRING>")
        return deepcopy(schema)
    found = set()

    def walk(fields, prefix=""):
        output = []
        for field in deepcopy(fields):
            path = prefix + field["name"]
            if path in SIMPLE_GROUPS:
                children = field.get("fields", [])
                numeric = path.endswith("quantidade_por_servico")
                if (
                    field["type"] not in {"RECORD", "STRUCT"}
                    or field.get("mode", "NULLABLE") != "NULLABLE"
                    or tuple(f["name"] for f in children) != SIMPLE_GROUPS[path]
                    or any(
                        f["type"]
                        not in ({"INTEGER", "INT64"} if numeric else {"STRING"})
                        or f.get("mode", "NULLABLE") != "NULLABLE"
                        or f.get("fields")
                        for f in children
                    )
                ):
                    raise ValueError("Agrupador incompatível: " + path)
                for child in children:
                    if numeric:
                        child["name"] = "quantidade_" + child["name"]
                    output.append(child)
                found.add(path)
            elif path == FORM_FIELDS_PATH:
                if (
                    field["type"] not in {"RECORD", "STRUCT"}
                    or field.get("mode") != "REPEATED"
                    or schema_signature(field.get("fields", []))
                    != schema_signature(
                        [
                            {"name": "label", "type": "STRING", "mode": "NULLABLE"},
                            {"name": "valor", "type": "STRING", "mode": "NULLABLE"},
                        ]
                    )
                ):
                    raise ValueError("Respostas de formulário incompatíveis")
                output.append({"name": "campos", "type": "STRING", "mode": "REPEATED"})
                found.add(path)
            else:
                if field.get("fields"):
                    field["fields"] = walk(field["fields"], path + ".")
                output.append(field)
        if len({f["name"] for f in output}) != len(output):
            raise ValueError("Colisão de nomes ao remover agrupadores")
        return output

    result = walk(schema)
    if found != set(SIMPLE_GROUPS) | {FORM_FIELDS_PATH}:
        raise ValueError("Schema anterior incompleto para migração v9")
    return result


def validate_simple_fields_schema(old, new):
    if schema_signature(simple_fields_schema(old)) != schema_signature(new):
        raise ValueError("Schema permite somente grupos simples e respostas em texto")


def simple_fields_projection(schema, alias="p", prefix="", level=0):
    """Forward projection of the complete frozen v8 row, retaining every array offset."""
    output = []
    for field in schema:
        name = field["name"]
        if not re.fullmatch(r"[a-zA-Z_][a-zA-Z0-9_]*", name):
            raise ValueError("Nome de coluna inválido no schema")
        logical = prefix + name
        path = f"{alias}.`{name}`"
        if logical in SIMPLE_GROUPS:
            for child in field["fields"]:
                target = (
                    "quantidade_" if logical.endswith("quantidade_por_servico") else ""
                ) + child["name"]
                output.append(f"{path}.`{child['name']}` AS `{target}`")
            continue
        if logical == FORM_FIELDS_PATH and field.get("fields"):
            expr = f"ARRAY(SELECT CONCAT(COALESCE(c.label, ''), ': ', COALESCE(c.valor, '')) FROM UNNEST({path}) c WITH OFFSET AS pos ORDER BY pos)"
        elif field.get("fields"):
            if field.get("mode") == "REPEATED":
                item, offset = f"v9item{level}", f"v9pos{level}"
                nested = simple_fields_projection(
                    field["fields"], item, logical + ".", level + 1
                )
                expr = f"ARRAY(SELECT AS STRUCT {nested} FROM UNNEST({path}) {item} WITH OFFSET AS {offset} ORDER BY {offset})"
            else:
                nested = simple_fields_projection(
                    field["fields"], path, logical + ".", level + 1
                )
                expr = f"IF({path} IS NULL, NULL, STRUCT({nested}))"
        else:
            expr = path
        output.append(f"{expr} AS `{name}`")
    return ", ".join(output)


def migration_parity_sql(
    before, after, schema=None, cpop_direct=False, simple_fields=False
):
    """Compare full rows through the explicitly selected schema transformation.

    Without schema, retain the historical Centro POP v2-to-v3 comparison only.
    """

    def normalized(relation, direct=False):
        if simple_fields:
            if schema is None:
                raise ValueError("Migração v9 exige schema anterior")
            projection = (
                schema_projection(simple_fields_schema(schema))
                if direct
                else simple_fields_projection(schema)
            )
            return f"SELECT p.usuario.id_usuario id_usuario, TO_JSON_STRING(STRUCT({projection})) valor FROM {quote(relation)} p"
        if schema is not None:
            return f"SELECT p.usuario.id_usuario id_usuario, TO_JSON_STRING(STRUCT({schema_projection(schema, cpop_direct=direct)})) valor FROM {quote(relation)} p"
        return f"""SELECT p.usuario.id_usuario id_usuario,
          TO_JSON_STRING((SELECT AS STRUCT p.* EXCEPT(escopos),
            STRUCT(STRUCT(p.escopos.centro_pop.historico AS historico,
              p.escopos.centro_pop.participacoes AS participacoes) AS centro_pop) AS escopos)) valor
          FROM {quote(relation)} p"""

    checks = []
    for relation in [before, after]:
        checks.append(f"""ASSERT (SELECT COUNT(*)=COUNT(DISTINCT usuario.id_usuario)
            AND COUNTIF(usuario.id_usuario IS NULL OR usuario.id_usuario='')=0 FROM {quote(relation)})
            AS 'População com chave inválida';""")
    checks.append(f"""ASSERT NOT EXISTS (
      WITH antes AS ({normalized(before)}), depois AS ({normalized(after, cpop_direct or simple_fields)})
      SELECT 1 FROM antes a FULL OUTER JOIN depois d USING(id_usuario)
      WHERE a.valor IS DISTINCT FROM d.valor
    ) AS 'Usuários, atendimentos, evoluções, histórico ou participações mudaram';""")
    return "\n".join(checks)


class Pipeline:
    def __init__(self, target="staging"):
        if target not in ["dev", "staging"]:
            raise ValueError(
                "Construção de escopos habilitada somente em staging/dev (rj-smas-dev)."
            )
        self.target = target
        profile = yaml.safe_load((ROOT / "profiles.yml").read_text())["rj_smas_dev"][
            "outputs"
        ][target]
        self.project = profile["project"]
        self.core = "core" if target == "prod" else profile["dataset"]
        self.report = "marts" if target == "prod" else "relatorio"
        credentials = None
        if target == "prod":
            import os

            from google.oauth2 import service_account

            credentials = service_account.Credentials.from_service_account_file(
                os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
            )
        self.client = bigquery.Client(
            project=self.project, credentials=credentials, location="US"
        )
        self.control = (
            f"{self.project}.{self.report}.controle_planilha_unificada_publicacoes"
        )
        self.destination = f"{self.project}.{self.report}.planilha_unificada"

    def query(self, sql, **params):
        config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter(k, "STRING", v) for k, v in params.items()
            ]
        )
        return list(self.client.query(sql, job_config=config).result())

    def dataset(self, name, kind, create=False):
        identifier(name, limit=256)
        path = f"{self.project}.{name}"
        if create:
            ds = bigquery.Dataset(path)
            ds.location = "US"
            ds.labels = {"pu_managed": kind, "pu_target": self.target}
            try:
                self.client.create_dataset(ds)
            except Conflict:
                pass
        actual = self.client.get_dataset(path)
        if (
            actual.labels.get("pu_managed") != kind
            or actual.labels.get("pu_target") != self.target
        ):
            raise ValueError(f"Dataset não pertence a esta operação: {path}")
        return path

    def relation(self, schema, name):
        if not re.fullmatch(r"[a-z_][a-z0-9_]{0,255}", name):
            raise ValueError(f"Nome de tabela inválido: {name}")
        return f"{self.project}.{identifier(schema, limit=256)}.{name}"

    def read_json(self, schema, name):
        rows = self.query(f"SELECT payload FROM {quote(self.relation(schema, name))}")
        if len(rows) != 1:
            raise ValueError(f"Metadados inválidos: {schema}.{name}")
        return json.loads(rows[0].payload)

    def write_json(self, schema, name, value, replace=False):
        mode = "OR REPLACE " if replace else ""
        self.query(
            f"CREATE {mode}TABLE {quote(self.relation(schema, name))} AS SELECT @payload AS payload",
            payload=json.dumps(
                value, ensure_ascii=False, default=stamp, sort_keys=True
            ),
        )

    def fingerprint(self, relation):
        table = self.client.get_table(relation)
        return {
            "etag": table.etag,
            "modified": stamp(table.modified),
            "rows": table.num_rows,
            "schema": [field.to_api_repr() for field in table.schema],
        }

    def verify(self, relation, expected):
        if self.fingerprint(relation) != expected:
            raise ValueError(f"Tabela mudou após a validação: {relation}")

    def dbt(self, command, key, variables=None, selection=None):
        artifact = ROOT / "target" / "pu" / key
        if command == "build":
            self.dbt("run", key, variables, selection)
            return self.dbt("test", key, variables, self.tests_for(artifact, selection))
        executable = Path(sys.executable).with_name("dbt")
        args = [
            str(executable),
            command,
            "--project-dir",
            str(ROOT),
            "--profiles-dir",
            str(ROOT),
            "--target",
            self.target,
            "--target-path",
            str(artifact),
            "--log-path",
            str(artifact / "logs"),
            "--no-partial-parse",
        ]
        if variables:
            args += ["--vars", json.dumps(variables)]
        if selection:
            args += ["--select", *selection, "--indirect-selection", "cautious"]
        subprocess.run(args, cwd=ROOT.parent, check=True)  # noqa: S603 - fixed executable, argv without shell
        return artifact

    def tests_for(self, artifact, selection):
        manifest = json.loads((artifact / "manifest.json").read_text())
        models = {"model.queries." + name for name in selection}
        tests = []
        for node in manifest["nodes"].values():
            if node["resource_type"] != "test":
                continue
            attached = node.get("attached_node") in models
            singular = (
                node.get("attached_node") is None
                and "model.queries.planilha_unificada" in models
                and "model.queries.planilha_unificada"
                in node.get("depends_on", {}).get("nodes", [])
            )
            if attached or singular:
                tests.append("fqn:" + ".".join(node["fqn"]))
        if not tests:
            raise ValueError(
                "Nenhum teste selecionado; candidato não pode ser aprovado."
            )
        return tests

    def prepare(self, batch, derive_from=None):
        if derive_from is not None:
            return self.derive_batch(batch, derive_from)
        if self.target not in ["dev", "staging"]:
            raise ValueError(
                "A captura da referência está habilitada somente em staging."
            )
        schema = "pu_base_" + identifier(batch)
        artifact = self.dbt("parse", f"preparar_{batch}", {"pu_referencia": True})
        manifest = json.loads((artifact / "manifest.json").read_text())
        nodes = {**manifest["nodes"], **manifest["sources"]}
        frontier, ancestors = {}, {}
        visited = set()

        def walk(key, boundary=False):
            if (key, boundary) in visited:
                return
            visited.add((key, boundary))
            node = nodes.get(key)
            if not node:
                return
            is_pu = node.get(
                "resource_type"
            ) != "source" and "/planilha_unificada/" in "/" + node.get(
                "original_file_path", ""
            )
            physical = node.get("resource_type") == "source" or (
                node.get("resource_type") == "model"
                and node["config"].get("materialized") != "ephemeral"
            )
            if physical and not is_pu:
                ancestors[key] = node
                if not boundary:
                    frontier[node["name"]] = node
                boundary = True
            for parent in node.get("depends_on", {}).get("nodes", []):
                walk(parent, boundary)

        for key, node in nodes.items():
            if "/planilha_unificada/" in "/" + node.get("original_file_path", ""):
                walk(key)
        if not frontier:
            raise ValueError("Manifesto não contém as entradas da planilha.")
        self.dataset(schema, "base", create=True)
        try:
            existing = self.read_json(schema, "_pu_lote")
        except NotFound:
            pass
        else:
            if existing.get("referencia", {}).get("versao") == version(
                self.fingerprint(PUBLIC)
            ):
                self.base(batch)
                print(f"Lote já selado: {schema}", flush=True)
                return
            raise ValueError(
                "Lote já selado com outra referência; use outro identificador."
            )
        with self.lock(schema, "preparar"):
            source_ids = {
                key: node["relation_name"].replace("`", "")
                for key, node in ancestors.items()
            }
            before = {key: self.fingerprint(rel) for key, rel in source_ids.items()}
            producer_fingerprint = self.fingerprint(PUBLIC)
            # Build the existing user entity and its ancestors in a private dataset.
            auxiliary = "pu_build_" + batch
            self.dataset(auxiliary, "base_aux", create=True)
            artifact_users = self.dbt(
                "run",
                f"{batch}/cadastro",
                {"pu_isolar_schema": auxiliary},
                ["+dim_usuarios", "raw_usuarios_enderecos"],
            )
            self.dbt(
                "test",
                f"{batch}/cadastro",
                {"pu_isolar_schema": auxiliary},
                self.tests_for(artifact_users, ["dim_usuarios"]),
            )
            # The test invocation replaces run_results; collect private physical relations via the manifest and existence.
            private_manifest = json.loads(
                (artifact_users / "manifest.json").read_text()
            )
            private_sources = {}
            for name in frontier:
                node = private_manifest["nodes"].get("model.queries." + name)
                if node and node["config"].get("materialized") != "ephemeral":
                    candidate_source = self.relation(auxiliary, name)
                    try:
                        self.client.get_table(candidate_source)
                    except NotFound:
                        pass
                    else:
                        private_sources[name] = candidate_source
            captured = self.query("SELECT CURRENT_TIMESTAMP() AS instante")[
                0
            ].instante.isoformat()
            snapshots = {}
            print(
                f"Capturando {len(frontier)} entradas; {len(ancestors)} dependências monitoradas",
                flush=True,
            )
            for name, node in sorted(frontier.items()):
                source = private_sources.get(
                    name, node["relation_name"].replace("`", "")
                )
                destination = self.relation(schema, name)
                table = self.client.get_table(source)
                # Views/external inputs are materialized and guarded by ancestor fingerprints.
                at = (
                    f" FOR SYSTEM_TIME AS OF TIMESTAMP({literal(captured)})"
                    if table.table_type == "TABLE"
                    else ""
                )
                self.query(
                    f"CREATE OR REPLACE TABLE {quote(destination)} AS SELECT * FROM {quote(source)}{at}"
                )
                snapshots[name] = self.fingerprint(destination)
                print(f"Capturado: {name}", flush=True)
            after = {key: self.fingerprint(rel) for key, rel in source_ids.items()}
            if before != after or self.fingerprint(PUBLIC) != producer_fingerprint:
                raise ValueError(
                    "Fontes mudaram durante a captura. Lote não foi selado; repita a preparação."
                )
            for model, key in [
                ("dim_usuarios", "id_usuario"),
                ("dim_unidades", "id_unidade"),
            ]:
                self.query(
                    f"ASSERT (SELECT COUNT(*)=COUNT(DISTINCT {key}) FROM {quote(self.relation(schema, model))}) AS 'Chave inválida na base';"
                )
            reference = self.relation(schema, "atendimentos_programada")
            self.query(f"""ASSERT NOT EXISTS(SELECT 1 FROM {quote(reference)} WHERE SEQ_ATENDIMENTO IS NULL OR modulo IS NULL OR modulo NOT IN ('u','f') OR ID_USUARIO IS NULL) AS 'PK inválida na referência';
                ASSERT NOT EXISTS(SELECT CONCAT(CAST(SEQ_ATENDIMENTO AS STRING),modulo) FROM {quote(reference)} GROUP BY 1 HAVING COUNT(DISTINCT TO_JSON_STRING(STRUCT(ID_USUARIO,DATA_DE_ATENDIMENTO,HORA_DE_ATENDIMENTO,UNIDADE_ATENDIMENTO,NOME_ATENDIMENTO_ORIGINAL))) != 1) AS 'Atributos conflitantes na referência';
                ASSERT NOT EXISTS(SELECT 1 FROM {quote(reference)} r LEFT JOIN {quote(self.relation(schema, "dim_usuarios"))} u ON r.ID_USUARIO=u.id_usuario WHERE u.id_usuario IS NULL) AS 'Usuário da referência ausente no cadastro atualizado';""")
            metrics = dict(
                self.query(
                    f"SELECT COUNT(*) linhas, COUNT(DISTINCT CONCAT(CAST(SEQ_ATENDIMENTO AS STRING),modulo)) eventos FROM {quote(reference)}"
                )[0].items()
            )
            reference_info = {
                "versao": version(producer_fingerprint),
                "origem": PUBLIC,
                "origem_fingerprint": producer_fingerprint,
                "capturado_em": captured,
                **metrics,
            }
            self.write_json(
                schema,
                "_pu_lote",
                {
                    "lote": batch,
                    "referencia": reference_info,
                    "capturado_em": captured,
                    "fontes": snapshots,
                    "origens": source_ids,
                    "metadados_origem": before,
                },
            )
        print(f"Lote selado: {schema}", flush=True)

    def derive_batch(self, batch, parent):
        batch, parent = identifier(batch), identifier(parent)
        if batch == parent:
            raise ValueError("Lote derivado exige um identificador novo")
        parent_schema, inherited = self.base(parent)
        schema = "pu_base_" + batch
        self.dataset(schema, "base", create=True)
        try:
            existing = self.read_json(schema, "_pu_lote")
        except NotFound:
            pass
        else:
            if existing.get("lote_pai") != parent:
                raise ValueError("Lote já selado com outra origem")
            self.base(batch)
            self.check_derived_batch(existing)
            print(f"Lote derivado já selado: {schema}", flush=True)
            return
        with self.lock(schema, "preparar"):
            parent_manifest = self.fingerprint(self.relation(parent_schema, "_pu_lote"))
            snapshots = {}
            for name in inherited["fontes"]:
                self.copy(
                    self.relation(parent_schema, name), self.relation(schema, name)
                )
                snapshots[name] = self.fingerprint(self.relation(schema, name))
                print(f"Herdado: {name}", flush=True)
            if "raw_usuarios_enderecos" in inherited["fontes"]:
                # Never replace an inherited snapshot with a fresh capture.
                self.base(parent)
                self.verify(self.relation(parent_schema, "_pu_lote"), parent_manifest)
                self.write_json(
                    schema,
                    "_pu_lote",
                    {
                        **inherited,
                        "lote": batch,
                        "lote_pai": parent,
                        "lote_pai_manifesto": parent_manifest,
                        "herdadas": inherited["fontes"],
                        "fontes": snapshots,
                        "derivado_em": datetime.now(timezone.utc).isoformat(),
                    },
                )
                print(
                    f"Lote derivado selado com endereços herdados: {schema}", flush=True
                )
                return
            auxiliary = "pu_build_" + batch
            self.dataset(auxiliary, "base_aux", create=True)
            artifact = self.dbt(
                "parse", f"{batch}/enderecos", {"pu_isolar_schema": auxiliary}
            )
            manifest = json.loads((artifact / "manifest.json").read_text())
            source_key = "source.queries.brutos_acolherio_staging.gh_cidadao_pac"
            source = manifest["sources"][source_key]["relation_name"].replace("`", "")
            original = self.fingerprint(source)
            self.dbt(
                "run",
                f"{batch}/enderecos",
                {"pu_isolar_schema": auxiliary},
                ["raw_usuarios_enderecos"],
            )
            captured = self.query("SELECT CURRENT_TIMESTAMP() instante")[
                0
            ].instante.isoformat()
            destination = self.relation(schema, "raw_usuarios_enderecos")
            self.query(
                f"CREATE OR REPLACE TABLE {quote(destination)} AS SELECT * FROM {quote(self.relation(auxiliary, 'raw_usuarios_enderecos'))}"
            )
            self.query(
                f"ASSERT (SELECT COUNT(*)=COUNT(DISTINCT id_paciente) AND COUNTIF(id_paciente IS NULL)=0 FROM {quote(destination)}) AS 'Cadastro de endereços conflitante';"
            )
            self.verify(source, original)
            self.base(parent)
            self.verify(self.relation(parent_schema, "_pu_lote"), parent_manifest)
            snapshots["raw_usuarios_enderecos"] = self.fingerprint(destination)
            self.write_json(
                schema,
                "_pu_lote",
                {
                    **inherited,
                    "lote": batch,
                    "lote_pai": parent,
                    "lote_pai_manifesto": parent_manifest,
                    "herdadas": inherited["fontes"],
                    "fontes": snapshots,
                    "derivado_em": captured,
                    "complementos": {
                        "raw_usuarios_enderecos": {
                            "capturado_em": captured,
                            "origem": source,
                            "origem_fingerprint": original,
                        }
                    },
                },
            )
        print(f"Lote derivado selado: {schema}", flush=True)

    def check_derived_batch(self, info):
        if not info.get("lote_pai"):
            return
        parent_schema, parent = self.base(info["lote_pai"])
        self.verify(
            self.relation(parent_schema, "_pu_lote"), info["lote_pai_manifesto"]
        )
        if (
            info.get("herdadas") != parent["fontes"]
            or info["referencia"] != parent["referencia"]
            or info["capturado_em"] != parent["capturado_em"]
        ):
            raise ValueError("Lote derivado não preserva a base e referência do pai")
        for name, original in parent["fontes"].items():
            clone = info["fontes"].get(name, {})
            if any(clone.get(k) != original[k] for k in ["rows", "schema"]):
                raise ValueError("Fonte herdada incompatível: " + name)

    def base(self, batch):
        schema = "pu_base_" + identifier(batch)
        self.dataset(schema, "base")
        info = self.read_json(schema, "_pu_lote")
        for name, expected in info["fontes"].items():
            self.verify(self.relation(schema, name), expected)
        return schema, info

    def context(self, batch, execution):
        base_schema, base = self.base(batch)
        schema = "pu_cand_" + identifier(execution)
        self.dataset(schema, "candidato", create=True)
        context = {
            "lote": batch,
            "execucao": execution,
            "capturado_em": base["capturado_em"],
            "referencia_versao": base["referencia"]["versao"],
            "contrato": UNIFIED_CONTRACT,
            "contrato_centro_pop": SCOPE_CONTRACT,
        }
        try:
            self.write_json(schema, "_pu_execucao", context)
        except Conflict:
            if self.read_json(schema, "_pu_execucao") != context:
                raise ValueError("Execução já pertence a outro lote.") from None
        variables = {
            "pu_base_schema": base_schema,
            "pu_referencia": True,
            "pu_fontes": dict.fromkeys(base["fontes"], True),
            "pu_candidato_schema": schema,
        }
        return schema, context, variables

    @contextmanager
    def lock(self, schema, operation):
        name = self.relation(schema, "_pu_lock_" + operation)
        self.query(
            f"CREATE TABLE {quote(name)} OPTIONS(expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 5 HOUR)) AS SELECT CURRENT_TIMESTAMP() AS criado_em"
        )
        try:
            yield
        finally:
            self.client.delete_table(name, not_found_ok=True)

    def build_dimension(self, batch, execution, service):
        if service != "centro_pop":
            raise ValueError(
                "Dimensões de eventos são legadas; nesta etapa somente o escopo centro_pop é construído."
            )
        schema, context, variables = self.context(batch, execution)
        required = {
            "raw_presencas_usuarios",
            "raw_evolucoes_questionario",
            "raw_evolucoes_questionario_lista",
        }
        if not required.issubset(variables["pu_fontes"]):
            raise ValueError(
                "Lote sem fontes do escopo Centro POP; prepare um novo lote completo."
            )
        with self.lock(schema, service):
            try:
                validated = self.read_json(schema, "_pu_validada_" + service)
            except NotFound:
                pass
            else:
                if validated.get("validacao") != SCOPE_CONTRACT:
                    raise ValueError(
                        "Artefato legado; use nova execução para o escopo de respostas."
                    )
                for relation, expected in validated["tabelas"].items():
                    self.verify(relation, expected)
                print("Escopo Centro POP já validado.", flush=True)
                return
            self.dbt(
                "build",
                f"{execution}/centro_pop",
                variables,
                ["dim_centro_pop", "int_pu_registros_centro_pop"],
            )
            self.base(batch)
            self.write_json(
                schema,
                "_pu_validada_centro_pop",
                {
                    **context,
                    "validacao": SCOPE_CONTRACT,
                    "tabelas": {
                        self.relation(schema, name): self.fingerprint(
                            self.relation(schema, name)
                        )
                        for name in ["dim_centro_pop", "int_pu_registros_centro_pop"]
                    },
                },
            )
        print(f"Escopo validado: {schema}.dim_centro_pop", flush=True)

    def state(self):
        try:
            rows = self.query(f"SELECT * FROM {quote(self.control)}")
        except NotFound:
            return {}
        result = {row.servico: dict(row.items()) for row in rows}
        if len(result) != len(rows):
            raise ValueError("Controle contém serviços duplicados.")
        return result

    def previous_dimensions(self, schema, state):
        result = {}
        if not state or state["supertabela"]["geracao"] == 0:
            return result
        for service, name in SERVICES.items():
            source = f"{self.project}.{self.core}.{name}"
            destination = self.relation(schema, "_pu_anterior_" + name)
            self.client.copy_table(
                source,
                destination,
                job_config=bigquery.CopyJobConfig(write_disposition="WRITE_TRUNCATE"),
            ).result()
            result[service] = destination
        return result

    def membership_sql(self, relations):
        parts = []
        for service, relation in relations.items():
            for block, key in [
                ("atendimentos", "id_atendimento_modulo"),
                ("evolucoes", "id_evolucao_modulo"),
            ]:
                parts.append(
                    f"SELECT CAST(d.id_usuario AS STRING) AS usuario, {literal(block)} AS bloco, "
                    f"r.{key} AS chave, {literal(service)} AS servico FROM {quote(relation)} AS d, UNNEST(d.{block}) AS r"
                )
        return " UNION ALL ".join(parts)

    def check_movements(self, base_schema, previous, selected):
        if not previous:
            return
        # Inspect all assignments in the new reference, including unselected services.
        source = reference_membership_sql(self.project, base_schema, quote)
        selected_sql = ",".join(literal(s) for s in selected)
        rows = self.query(f"""WITH anterior AS ({self.membership_sql(previous)}), novo AS ({source})
          SELECT DISTINCT a.servico AS anterior, n.servico AS novo
          FROM anterior a JOIN novo n ON a.bloco=n.bloco AND a.chave=n.chave
            AND (a.bloco='atendimentos' OR a.usuario=n.usuario)
          WHERE a.servico != n.servico
            AND (a.servico IN ({selected_sql}) OR n.servico IN ({selected_sql}))
            AND NOT (a.servico IN ({selected_sql}) AND n.servico IN ({selected_sql}))""")
        if rows:
            raise ValueError(
                "Mudança de serviço: publique as dimensões envolvidas juntas: "
                + str([dict(r.items()) for r in rows])
            )

    def migration_baseline(self, schema, batch, state):
        if state.get("supertabela", {}).get("lote") != batch:
            _, child = self.base(batch)
            if child.get("lote_pai") != state.get("supertabela", {}).get("lote"):
                raise ValueError(
                    "Migração exige o mesmo lote congelado ou seu derivado validado."
                )
            self.check_derived_batch(child)
        live = (
            [self.destination, self.control]
            + [
                f"{self.project}.{self.core}.{name}"
                for name in [
                    "dim_centro_pop",
                    "int_pu_registros_centro_pop",
                    "dim_cras",
                    "dim_creas",
                    "dim_outros_equipamentos",
                ]
            ]
            + [f"{self.project}.relatorio.mart_planilha_unificada_centro_pop"]
        )
        fingerprints = {r: self.fingerprint(r) for r in live}
        baseline = self.relation(schema, "_pu_antes_planilha_unificada")
        self.copy(self.destination, baseline)
        for relation, expected in fingerprints.items():
            self.verify(relation, expected)
        return {
            "tabela": baseline,
            "fingerprint": self.fingerprint(baseline),
            "publicadas": fingerprints,
        }

    def copy(self, source, destination):
        self.client.copy_table(
            source,
            destination,
            job_config=bigquery.CopyJobConfig(write_disposition="WRITE_TRUNCATE"),
        ).result()

    def consolidate(self, batch, execution, selected):
        if set(selected) != {"centro_pop"}:
            raise ValueError(
                "A consolidação atual utiliza somente o escopo centro_pop; eventos vêm dos intermediários comuns."
            )
        schema, context, variables = self.context(batch, execution)
        with self.lock(schema, "consolidar_publicar"):
            info = self.read_json(schema, "_pu_validada_centro_pop")
            if info.get("validacao") != SCOPE_CONTRACT or info.get("lote") != batch:
                raise ValueError(
                    "Escopo legado ou de outro lote; reconstrua a dimensão na nova execução."
                )
            for relation, expected in info["tabelas"].items():
                self.verify(relation, expected)
            if "raw_usuarios_enderecos" not in variables.get("pu_fontes", {}):
                raise ValueError(
                    "Lote sem snapshot cadastral dos endereços; prepare um lote derivado."
                )
            state = self.state()
            base_schema, base = self.base(batch)
            baseline = self.migration_baseline(schema, batch, state)
            self.dbt(
                "build",
                f"{execution}/consolidar",
                variables,
                ["int_pu_enderecos_bi", "int_pu_complementos_bi", "planilha_unificada"],
            )
            candidate = self.relation(schema, "planilha_unificada")
            self.query(
                parity_sql(
                    self.relation(base_schema, "atendimentos_programada"),
                    candidate,
                    quote,
                    direct_counts=True,
                )
            )
            self.validate_candidate_schema(
                candidate, baseline["fingerprint"].get("schema")
            )
            self.query(
                migration_parity_sql(
                    baseline["tabela"],
                    candidate,
                    baseline["fingerprint"].get("schema"),
                    simple_fields=True,
                )
            )
            self.verify(baseline["tabela"], baseline["fingerprint"])
            for relation, expected in baseline["publicadas"].items():
                self.verify(relation, expected)
            self.base(batch)
            for relation, expected in info["tabelas"].items():
                self.verify(relation, expected)
            if self.state() != state:
                raise ValueError(
                    "Estado publicado mudou durante a validação; reconsolide."
                )
            self.write_json(
                schema,
                "_pu_consolidada",
                {
                    **context,
                    "contrato": UNIFIED_CONTRACT,
                    "publicacao_permitida": True,
                    "antes_migracao": baseline,
                    "geracao": state.get("supertabela", {}).get("geracao", 0),
                    "referencia": base["referencia"],
                    "cadastro": {
                        "lote": batch,
                        "execucao": execution,
                        "capturado_em": base["capturado_em"],
                    },
                    "escopos": {"centro_pop": info},
                    "tabelas": info["tabelas"],
                    "tabela": self.fingerprint(candidate),
                },
                replace=True,
            )
        print(
            f"Unificada com campos diretos e formulários em texto validada: {candidate}; pronta para publicação manual com backup.",
            flush=True,
        )

    def initialize_control(self):
        if self.state():
            return
        # One CTAS creates the complete initial control atomically, without an empty interval.
        try:
            self.query(f"""CREATE TABLE {quote(self.control)} AS
                SELECT servico, CAST(NULL AS STRING) AS lote, CAST(NULL AS STRING) AS execucao,
                CAST(NULL AS TIMESTAMP) AS capturado_em, CAST(NULL AS TIMESTAMP) AS publicado_em,
                0 AS geracao, 'pronto' AS estado
                FROM UNNEST(['supertabela','cras','creas','centro_pop','outros']) AS servico""")
        except Conflict:
            pass

    def ensure_public_schema(self, candidate):
        current = self.client.get_table(self.destination)
        expected = self.client.get_table(candidate)
        if current.schema == expected.schema:
            return False
        allowed = {
            "atendimentos.registros.tipo_servico",
            "evolucoes.registros.tipo_servico",
        }

        def compatible(old, new, path=""):
            if len(new) < len(old):
                return False
            for left, right in zip(old, new):
                if (left.name, left.field_type, left.mode) != (
                    right.name,
                    right.field_type,
                    right.mode,
                ):
                    return False
                if not compatible(left.fields, right.fields, path + left.name + "."):
                    return False
            return all(
                path + field.name in allowed
                and field.field_type == "STRING"
                and field.mode == "NULLABLE"
                for field in new[len(old) :]
            )

        if not compatible(current.schema, expected.schema):
            raise ValueError(
                "Migração de schema não aditiva; revise antes de publicar."
            )
        # Only additive nested STRING fields; rollback restores the backup if DML fails.
        current.schema = expected.schema
        self.client.update_table(current, ["schema"])
        return True

    def validate_candidate_schema(self, candidate, previous):
        if previous is None:
            raise ValueError("Manifesto sem schema anterior")
        current = [f.to_api_repr() for f in self.client.get_table(candidate).schema]
        validate_simple_fields_schema(previous, current)

    def assert_paused(self):
        producer = Producer()
        if not producer.get().get("disabled", False) or producer.active_runs():
            raise ValueError(
                "Consulta programada deve permanecer pausada e sem execuções pendentes."
            )

    def publish(self, execution):
        schema = "pu_cand_" + identifier(execution)
        self.dataset(schema, "candidato")
        self.dataset("pu_coordenacao", "coordenacao", create=True)
        with (
            self.lock("pu_coordenacao", "publicacao"),
            self.lock(schema, "consolidar_publicar"),
        ):
            info = self.read_json(schema, "_pu_consolidada")
            if info.get("contrato") != UNIFIED_CONTRACT or not info.get(
                "publicacao_permitida"
            ):
                raise ValueError("Candidato exige nova consolidação no contrato v9.")
            state = self.state()
            if state.get("supertabela", {}).get("geracao") != info["geracao"]:
                raise ValueError("Geração mudou; reconsolide antes de publicar.")
            self.assert_paused()
            _, frozen = self.base(info["lote"])
            self.check_derived_batch(frozen)
            candidate = self.relation(schema, "planilha_unificada")
            self.verify(candidate, info["tabela"])
            for relation, expected in info["tabelas"].items():
                self.verify(relation, expected)
            baseline = info["antes_migracao"]
            self.verify(baseline["tabela"], baseline["fingerprint"])
            for relation, expected in baseline["publicadas"].items():
                self.verify(relation, expected)
            self.validate_candidate_schema(
                candidate, baseline["fingerprint"].get("schema")
            )
            self.query(
                migration_parity_sql(
                    baseline["tabela"],
                    candidate,
                    baseline["fingerprint"].get("schema"),
                    simple_fields=True,
                )
            )
            self.query(parity_sql(PUBLIC, candidate, quote, direct_counts=True))
            replacements = [(self.destination, candidate)]
            suffix = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
            backups = {}
            for destination in [*(d for d, _ in replacements), self.control]:
                backup = self.relation(
                    schema,
                    "_pu_backup_" + destination.rsplit(".", 1)[-1] + "_" + suffix,
                )
                self.copy(destination, backup)
                backups[destination] = backup
            # All backups and the recovery manifest exist before the first live replacement.
            recovery = {
                "contrato": UNIFIED_CONTRACT,
                "execucao": execution,
                "backups": backups,
                "estado": "preparado",
            }
            self.write_json(schema, "_pu_migracao", recovery, replace=True)
            for relation, expected in baseline["publicadas"].items():
                self.verify(relation, expected)
            self.assert_paused()
            changed = []
            try:
                for destination, source in replacements:
                    changed.append(destination)
                    self.copy(source, destination)
                    self.query(table_equality_sql(source, destination))
                self.query(
                    migration_parity_sql(
                        baseline["tabela"],
                        self.destination,
                        baseline["fingerprint"].get("schema"),
                        simple_fields=True,
                    )
                )
                self.query(
                    parity_sql(PUBLIC, self.destination, quote, direct_counts=True)
                )
                for relation, expected in baseline["publicadas"].items():
                    if relation not in {self.destination, self.control}:
                        self.verify(relation, expected)
                self.assert_paused()
                changed.append(self.control)
                self.query(
                    f"""BEGIN TRANSACTION;
                    ASSERT (SELECT COUNT(*)=1 FROM {quote(self.control)} WHERE servico='supertabela'
                      AND geracao={int(info["geracao"])} AND estado='pronto') AS 'Geração mudou';
                    UPDATE {quote(self.control)} SET lote=@lote,execucao=@execucao,
                      capturado_em=TIMESTAMP(@capturado),publicado_em=CURRENT_TIMESTAMP(),
                      geracao={int(info["geracao"]) + 1},estado='pronto',contrato=@contrato,
                      referencia_versao=@referencia
                    WHERE servico='supertabela';
                    UPDATE {quote(self.control)} SET escopos_utilizados=PARSE_JSON(@escopos)
                    WHERE servico='supertabela';
                    COMMIT TRANSACTION;""",
                    lote=info["lote"],
                    execucao=execution,
                    capturado=info["cadastro"]["capturado_em"],
                    contrato=UNIFIED_CONTRACT,
                    referencia=info["referencia"]["versao"],
                    escopos=json.dumps(info["escopos"]),
                )
                self.write_json(
                    schema,
                    "_pu_migracao",
                    {**recovery, "estado": "publicado"},
                    replace=True,
                )
            except Exception as error:
                failures = []
                for destination in reversed(changed):
                    try:
                        self.copy(backups[destination], destination)
                        self.query(
                            table_equality_sql(backups[destination], destination)
                        )
                    except Exception as restore_error:
                        failures.append(f"{destination}: {restore_error}")
                self.write_json(
                    schema,
                    "_pu_migracao",
                    {
                        **recovery,
                        "estado": "restauracao_pendente" if failures else "restaurado",
                        "erro": str(error),
                        "falhas_restauracao": failures,
                    },
                    replace=True,
                )
                if failures:
                    raise RuntimeError(
                        "Falha na restauração; usar backups do manifesto: "
                        + str(failures)
                    ) from error
                raise
        print(
            f"Publicado: {self.destination}; backups em {schema}._pu_migracao; programada permanece pausada.",
            flush=True,
        )

    def cycle(self):
        raise ValueError(
            "Ciclo automático desabilitado. Use preparar, dimensao e consolidar; publicar é uma operação manual explícita."
        )

    def clean(self, schemas):
        state = self.state()
        referenced = {"pu_base_" + r["lote"] for r in state.values() if r["lote"]}
        referenced |= {
            "pu_cand_" + r["execucao"] for r in state.values() if r["execucao"]
        }
        # Pending candidates retain their common input batches too.
        for dataset in self.client.list_datasets():
            if (
                dataset.labels.get("pu_managed") == "candidato"
                and dataset.dataset_id not in schemas
            ):
                try:
                    referenced.add(
                        "pu_base_"
                        + self.read_json(dataset.dataset_id, "_pu_execucao")["lote"]
                    )
                except NotFound:
                    pass
                try:
                    pending = self.read_json(dataset.dataset_id, "_pu_consolidada")
                    referenced.add("pu_base_" + pending["cadastro"]["lote"])
                    for item in {
                        **pending.get("dimensoes", {}),
                        **pending.get("escopos", {}),
                    }.values():
                        referenced.add("pu_base_" + item["lote"])
                        referenced.add("pu_cand_" + item["execucao"])
                except NotFound:
                    pass
        frontier = [s for s in referenced if s.startswith("pu_base_")]
        visited = set()
        while frontier:
            base_schema = frontier.pop()
            if base_schema in visited:
                continue
            visited.add(base_schema)
            metadata = self.read_json(base_schema, "_pu_lote")
            parent = metadata.get("lote_pai")
            if parent:
                parent_schema = "pu_base_" + identifier(parent)
                referenced.add(parent_schema)
                frontier.append(parent_schema)
        for schema in schemas:
            kind = (
                "base_aux"
                if schema.startswith("pu_build_")
                else ("base" if schema.startswith("pu_base_") else "candidato")
            )
            path = self.dataset(schema, kind)
            if schema in referenced:
                raise ValueError(f"Dataset ainda referenciado: {schema}")
            if kind == "candidato":
                try:
                    recovery = self.read_json(schema, "_pu_migracao")
                except NotFound:
                    pass
                else:
                    if recovery.get("estado") in {"preparado", "restauracao_pendente"}:
                        raise ValueError(
                            f"Dataset contém backups de migração pendente: {schema}"
                        )
            if any(
                t.table_id.startswith("_pu_lock_")
                for t in self.client.list_tables(path)
            ):
                raise ValueError(f"Dataset com operação em andamento: {schema}")
        for schema in schemas:
            self.client.delete_dataset(f"{self.project}.{schema}", delete_contents=True)
            print(f"Candidato/lote sem referências removido: {schema}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--target", choices=["dev", "staging", "prod"], default="staging"
    )
    commands = parser.add_subparsers(dest="command", required=True)
    prepare = commands.add_parser("preparar")
    prepare.add_argument("--lote", required=True, type=identifier)
    prepare.add_argument("--derivar-de", type=identifier)
    for name in ["dimensao", "consolidar"]:
        command = commands.add_parser(name)
        command.add_argument("--lote", required=True, type=identifier)
        command.add_argument("--execucao", required=True, type=identifier)
        if name == "dimensao":
            command.add_argument("--servico", required=True, choices=SERVICES)
        else:
            command.add_argument(
                "--servicos", nargs="+", required=True, choices=SERVICES
            )
    publish = commands.add_parser("publicar")
    publish.add_argument("--execucao", required=True, type=identifier)
    clean = commands.add_parser("limpar")
    clean.add_argument("--datasets", nargs="+", required=True, type=identifier)
    commands.add_parser("ciclo")
    commands.add_parser("semear-referencia")
    args = parser.parse_args()
    pipeline = Pipeline(args.target)
    if args.command == "semear-referencia":
        try:
            pipeline.client.get_table(CANDIDATE)
        except NotFound:
            pipeline.client.copy_table(PUBLIC, CANDIDATE).result()
        print("Tabela candidata da programada disponível.")
    elif args.command == "ciclo":
        pipeline.cycle()
    elif args.command == "preparar":
        pipeline.prepare(args.lote, args.derivar_de)
    elif args.command == "dimensao":
        pipeline.build_dimension(args.lote, args.execucao, args.servico)
    elif args.command == "consolidar":
        pipeline.consolidate(
            args.lote, args.execucao, list(dict.fromkeys(args.servicos))
        )
    elif args.command == "publicar":
        pipeline.publish(args.execucao)
    else:
        pipeline.clean(args.datasets)


if __name__ == "__main__":
    main()
