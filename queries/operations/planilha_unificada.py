"""Build, validate and publish PU batches, without changing shared dbt models.

Run with the repository Python environment. `--help` and docs/dimensoes_planilha_unificada.md
explain the staged commands. All dbt artifacts are kept under queries/target/pu.
"""

# SQL templates use quote()/literal() for validated relations and scalar literals; JSON payloads use parameters.
# ruff: noqa: S608
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import datetime
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
        version,
    )
except ModuleNotFoundError:  # CLI invoked with queries/operations as sys.path[0]
    from referencia_programada import CANDIDATE, PUBLIC, Producer, parity_sql, version

ROOT = Path(__file__).resolve().parents[1]
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
    parts += ["COMMIT TRANSACTION;"]
    return "\n".join(parts)


class Pipeline:
    def __init__(self, target="staging"):
        if target not in ["dev", "staging"]:
            raise ValueError(
                "Publicação pareada habilitada somente em staging/dev (rj-smas-dev)."
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

    def prepare(self, batch):
        if self.target not in ["dev", "staging"]:
            raise ValueError(
                "A referência coordenada está habilitada somente em staging."
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
                self.fingerprint(CANDIDATE)
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
            producer_fingerprint = self.fingerprint(CANDIDATE)
            # Build the existing user entity and its ancestors in a private dataset.
            auxiliary = "pu_build_" + batch
            self.dataset(auxiliary, "base_aux", create=True)
            artifact_users = self.dbt(
                "run",
                f"{batch}/cadastro",
                {"pu_isolar_schema": auxiliary},
                ["+dim_usuarios"],
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
            if before != after or self.fingerprint(CANDIDATE) != producer_fingerprint:
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
                "origem": CANDIDATE,
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
        schema, context, variables = self.context(batch, execution)
        with self.lock(schema, service):
            try:
                validated = self.read_json(schema, "_pu_validada_" + service)
            except NotFound:
                pass
            else:
                self.verify(
                    self.relation(schema, SERVICES[service]), validated["tabela"]
                )
                if validated.get("validacao") == "programada_v2":
                    print(f"Dimensão já validada: {service}")
                    return
                artifact = ROOT / "target" / "pu" / execution / service
                self.dbt(
                    "test",
                    f"{execution}/{service}",
                    variables,
                    self.tests_for(artifact, [SERVICES[service]]),
                )
                validated["validacao"] = "programada_v2"
                self.write_json(
                    schema, "_pu_validada_" + service, validated, replace=True
                )
                return
            self.dbt("build", f"{execution}/{service}", variables, [SERVICES[service]])
            self.base(batch)
            context["validacao"] = "programada_v2"
            context["tabela"] = self.fingerprint(
                self.relation(schema, SERVICES[service])
            )
            self.write_json(schema, "_pu_validada_" + service, context)
        print(f"Dimensão validada: {schema}.{SERVICES[service]}", flush=True)

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
        # Ownership is read from the entire new source batch, including services not yet built.
        fact = quote(self.relation(base_schema, "fct_atendimentos"))
        evo = quote(self.relation(base_schema, "int_evolucoes"))
        units = quote(self.relation(base_schema, "dim_unidades"))
        users = quote(self.relation(base_schema, "dim_usuarios"))
        classify = "CASE u.tipo_unidade WHEN 'CRAS' THEN 'cras' WHEN 'CREAS' THEN 'creas' WHEN 'Centro POP' THEN 'centro_pop' ELSE 'outros' END"
        source = f"""
          SELECT DISTINCT CAST(a.id_usuario AS STRING) AS usuario, 'atendimentos' AS bloco,
            a.id_atendimento_modulo AS chave, {classify} AS servico
          FROM {fact} a JOIN {users} c ON a.id_usuario=c.id_usuario
          LEFT JOIN {units} u ON a.id_unidade=u.id_unidade
          WHERE UPPER(TRIM(COALESCE(a.flag_cancelado,''))) != 'S'
          UNION DISTINCT
          SELECT CAST(COALESCE(a.id_usuario,a.id_paciente_familia) AS STRING), 'evolucoes',
            CONCAT(a.origem_modulo,':',CAST(a.id_evolucao AS STRING)), {classify}
          FROM {evo} a JOIN {users} c ON COALESCE(a.id_usuario,a.id_paciente_familia)=c.id_usuario
          LEFT JOIN {units} u ON a.id_unidade=u.id_unidade WHERE a.data_cancelamento IS NULL
        """
        selected_sql = ",".join(literal(s) for s in selected)
        rows = self.query(f"""WITH anterior AS ({self.membership_sql(previous)}), novo AS ({source})
          SELECT DISTINCT a.servico AS anterior, n.servico AS novo
          FROM anterior a JOIN novo n USING(usuario,bloco,chave)
          WHERE a.servico != n.servico
            AND (a.servico IN ({selected_sql}) OR n.servico IN ({selected_sql}))
            AND NOT (a.servico IN ({selected_sql}) AND n.servico IN ({selected_sql}))""")
        if rows:
            raise ValueError(
                "Mudança de serviço: publique as dimensões envolvidas juntas: "
                + str([dict(r.items()) for r in rows])
            )

    def consolidate(self, batch, execution, selected):
        schema, context, _ = self.context(batch, execution)
        with self.lock(schema, "consolidar_publicar"):
            state = self.state()
            root = state.get("supertabela", {})
            generation = root.get("geracao", 0)
            if root.get("estado", "pronto") != "pronto":
                raise ValueError(
                    "Inicialização em andamento; conclua a publicação anterior."
                )
            if set(selected) != set(SERVICES):
                raise ValueError("Publicação pareada exige as quatro dimensões.")
            selected_info = {}
            for service in selected:
                info = self.read_json(schema, "_pu_validada_" + service)
                if info.get("validacao") != "programada_v2":
                    raise ValueError(
                        f"Revalide a dimensão com o comando dimensao: {service}"
                    )
                self.verify(self.relation(schema, SERVICES[service]), info["tabela"])
                old = state.get(service, {})
                if (
                    old.get("capturado_em")
                    and datetime.fromisoformat(info["capturado_em"])
                    < old["capturado_em"]
                ):
                    raise ValueError(
                        f"Lote mais antigo que a versão publicada: {service}"
                    )
                if (
                    info.get("referencia_versao") != context["referencia_versao"]
                    or info.get("lote") != batch
                ):
                    raise ValueError(
                        "Dimensões pertencem a referências/lotes diferentes."
                    )
                selected_info[service] = info
            cadastro_batch = batch
            base_schema, base = self.base(cadastro_batch)
            relations = {s: self.relation(schema, name) for s, name in SERVICES.items()}
            variables = {
                "pu_base_schema": base_schema,
                "pu_referencia": True,
                "pu_fontes": dict.fromkeys(base["fontes"], True),
                "pu_candidato_schema": schema,
                "pu_dimensoes": {s: quote(rel) for s, rel in relations.items()},
            }
            self.dbt(
                "build", f"{execution}/consolidar", variables, ["planilha_unificada"]
            )
            self.query(
                parity_sql(
                    self.relation(base_schema, "atendimentos_programada"),
                    self.relation(schema, "planilha_unificada"),
                    quote,
                )
            )
            if self.state() != state:
                raise ValueError(
                    "Publicação mudou durante a consolidação; reconsolide."
                )
            for service, info in selected_info.items():
                self.verify(relations[service], info["tabela"])
            self.base(cadastro_batch)
            self.write_json(
                schema,
                "_pu_consolidada",
                {
                    **context,
                    "geracao": generation,
                    "referencia": base["referencia"],
                    "servicos": list(selected),
                    "cadastro": {
                        "lote": cadastro_batch,
                        "execucao": execution,
                        "capturado_em": base["capturado_em"],
                    },
                    "dimensoes": selected_info,
                    "relacoes": relations,
                    "tabelas": {
                        rel: self.fingerprint(rel) for rel in relations.values()
                    },
                    "tabela": self.fingerprint(
                        self.relation(schema, "planilha_unificada")
                    ),
                },
                replace=True,
            )
        print(
            f"Supertabela validada: {schema}.planilha_unificada (geração de origem {generation})",
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

    def publish(self, execution):
        schema = "pu_cand_" + identifier(execution)
        self.dataset(schema, "candidato")
        self.dataset("pu_coordenacao", "coordenacao", create=True)
        with (
            self.lock("pu_coordenacao", "publicacao"),
            self.lock(schema, "consolidar_publicar"),
        ):
            info = self.read_json(schema, "_pu_consolidada")
            if not info.get("referencia") or set(info["servicos"]) != set(SERVICES):
                raise ValueError(
                    "Publicação exige quatro dimensões e referência programada validada."
                )
            self.verify(self.relation(schema, "planilha_unificada"), info["tabela"])
            for relation, expected in info["tabelas"].items():
                self.verify(relation, expected)
            early_state = self.state()
            early_root = early_state.get(
                "supertabela", {"geracao": 0, "execucao": None, "estado": "pronto"}
            )
            if early_root["execucao"] == execution and early_root["estado"] == "pronto":
                print("Publicação já concluída; nenhuma alteração.")
                return
            if early_root["geracao"] != info["geracao"]:
                raise ValueError("Geração mudou; reconsolide antes de publicar.")
            base_schema, base = self.base(info["lote"])
            reference = self.relation(base_schema, "atendimentos_programada")
            self.query(
                parity_sql(
                    reference, self.relation(schema, "planilha_unificada"), quote
                )
            )
            Producer().assert_routed()  # Never leave an independent writer on the public destination.
            self.initialize_control()
            self.query(
                f"ALTER TABLE {quote(self.control)} ADD COLUMN IF NOT EXISTS referencia_versao STRING;"
            )
            self.query(f"""MERGE {quote(self.control)} t USING(SELECT 'programada' servico) s ON t.servico=s.servico
              WHEN NOT MATCHED THEN INSERT(servico,geracao,estado) VALUES('programada',0,'pronto');""")
            state = self.state()
            root = state["supertabela"]
            if root["execucao"] == execution and root["estado"] == "pronto":
                print("Publicação já concluída; nenhuma alteração.")
                return
            if root["geracao"] != info["geracao"]:
                raise ValueError("Geração mudou; reconsolide antes de publicar.")
            dimensions = [
                (f"{self.project}.{self.core}.{name}", self.relation(schema, name))
                for name in SERVICES.values()
            ]
            for published, candidate in dimensions + [
                (self.destination, self.relation(schema, "planilha_unificada")),
                (PUBLIC, reference),
            ]:
                self.query(
                    f"CREATE TABLE IF NOT EXISTS {quote(published)} AS SELECT * FROM {quote(candidate)} WHERE FALSE"
                )
            # Retain a paired pre-publication backup for rollback/audit.
            for name, source in [
                ("_pu_backup_unificada", self.destination),
                ("_pu_backup_programada", PUBLIC),
            ]:
                backup = self.relation(schema, name)
                try:
                    self.client.get_table(backup)
                except NotFound:
                    self.client.copy_table(source, backup).result()
            updates = {**info["dimensoes"], "programada": info["cadastro"]}
            self.query(
                transaction_sql(
                    self.control,
                    self.destination,
                    self.relation(schema, "planilha_unificada"),
                    dimensions,
                    info["geracao"],
                    updates,
                    info["cadastro"],
                    reference=reference,
                    programada=PUBLIC,
                    reference_version=info["referencia"]["versao"],
                )
            )
        print(
            f"Publicação pareada concluída: {self.destination} e {PUBLIC}", flush=True
        )

    def cycle(self):
        Producer().assert_routed()
        self.dataset("pu_coordenacao", "coordenacao", create=True)
        with self.lock("pu_coordenacao", "ciclo"):
            fingerprint = self.fingerprint(CANDIDATE)
            ref_version = version(fingerprint)
            if (
                self.state().get("programada", {}).get("referencia_versao")
                == ref_version
            ):
                print("Referência já publicada; nenhuma alteração.", flush=True)
                return
            batch = "b" + ref_version[:24]
            execution = "e" + ref_version[:24]
            self.prepare(batch)
            # Four isolated dbt subprocesses; never share dbtRunner across threads.
            with ThreadPoolExecutor(max_workers=4) as pool:
                futures = [
                    pool.submit(self.build_dimension, batch, execution, s)
                    for s in SERVICES
                ]
                for future in futures:
                    future.result()
            self.consolidate(batch, execution, list(SERVICES))
            self.publish(execution)

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
                except NotFound:
                    pass
        for schema in schemas:
            kind = (
                "base_aux"
                if schema.startswith("pu_build_")
                else ("base" if schema.startswith("pu_base_") else "candidato")
            )
            path = self.dataset(schema, kind)
            if schema in referenced:
                raise ValueError(f"Dataset ainda referenciado: {schema}")
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
        pipeline.prepare(args.lote)
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
