# ruff: noqa: S608
# Relações vêm de constantes, UUIDs, metadados BigQuery e do manifesto dbt.
"""Reconstrução isolada e publicação do prontuário."""
import csv
import io
import json
import logging
from pathlib import Path
import re
import shutil
import subprocess
import sys
from tempfile import TemporaryDirectory
from urllib.parse import urlsplit
from urllib.request import urlopen
from uuid import UUID

from google.api_core.exceptions import Conflict, NotFound
from google.cloud import bigquery

PROJECT = "rj-smas-dev"
PROD_PROJECT = "rj-smas"
LOCATION = "US"
DESTINATION = f"{PROJECT}.abordagem.prontuario_eletronico"
SHEET = f"{PROJECT}.dashboard_acolherio.filtro_email_dev"
QUERIES = Path(__file__).resolve().parents[4] / "queries"
MART = "prontuario_eletronico"
PUBLISHED_MART = "prontuario_cartoes"
ATTENDANCE_SELECTION = "int_atendimentos_selecionados"
PERMISSIONS = "int_filtro_email_unidades"
TEST_PATH = "tests/prontuario_eletronico/"
LOGGER = logging.getLogger(__name__)


def execute(client, sql, report, phase, *, config=None, job_id=None):
    report["etapa"] = phase
    LOGGER.info("Prontuário: %s", phase)
    job_options = {"job_id": job_id, "job_retry": None} if job_id is not None else {}
    try:
        job = client.query(sql, job_config=config, **job_options)
    except Conflict:
        if job_id is None:
            raise
        job = client.get_job(job_id, location=LOCATION)
    report.setdefault("jobs", {})[phase] = job.job_id
    return job.result()


def load_sheet(client, destination, report):
    uri, = client.get_table(SHEET).external_data_configuration.source_uris
    parsed = urlsplit(uri)
    parts = parsed.path.split("/")
    if (parsed.scheme != "https" or parsed.hostname != "docs.google.com" or len(parts) < 4
            or parts[:3] != ["", "spreadsheets", "d"] or not re.fullmatch(r"[\w-]+", parts[3])):
        raise ValueError("Origem inesperada da planilha")
    url = f"https://docs.google.com/spreadsheets/d/{parts[3]}/export?format=csv&gid=0"
    with urlopen(url, timeout=30) as response:  # noqa: S310
        csv_text = response.read().decode("utf-8-sig")
    rows = [row for row in csv.reader(io.StringIO(csv_text), strict=True) if any(row)]
    if not rows or any(len(row) != 2 or not row[0].strip() or row[0].strip().casefold() == "unidade_atendimento" for row in rows):
        raise ValueError("A planilha deve conter unidade e e-mail, sem cabeçalho")
    records = [dict(unidade_atendimento=unit, email=email) for unit, email in rows]
    config = bigquery.LoadJobConfig(schema=[
        bigquery.SchemaField("unidade_atendimento", "STRING"),
        bigquery.SchemaField("email", "STRING"),
    ])
    job = client.load_table_from_json(records, destination, job_config=config)
    report.setdefault("jobs", {})["planilha"] = job.job_id
    job.result()


def ordered_tables(manifest):
    """Percorre os refs, inclusive os modelos ephemeral entre tabelas."""
    seen = set()
    models = []

    def visit(key):
        if key in seen:
            return
        seen.add(key)
        for parent in manifest["parent_map"].get(key, []):
            visit(parent)
        node = manifest["nodes"].get(key)
        if node and node["resource_type"] == "model" and node["config"]["materialized"] == "table":
            models.append(key)

    root = f"model.queries.{PUBLISHED_MART}"
    if root not in manifest["nodes"]:
        raise RuntimeError("Modelo dbt de projeção dos cartões não encontrado")
    visit(root)
    return models


def dbt_command(command, directory, target="staging"):
    dbt = Path(sys.executable).with_name("dbt")
    executable = str(dbt) if dbt.exists() else shutil.which("dbt") or "dbt"
    return [
        executable, "--quiet", command, "--project-dir", str(QUERIES),
        "--profiles-dir", str(QUERIES), "--target", target, "--target-path", directory,
    ]


def compile_graph(target="staging"):
    with TemporaryDirectory(prefix="prontuario-dbt-") as directory:
        manifest_file = Path(directory) / "manifest.json"
        variables = ["--vars", json.dumps({
            "prontuario_projetar_cartoes": True,
            "prontuario_publicacao": target == "prod",
        })]
        subprocess.run([*dbt_command("parse", directory, target), *variables], check=True, stdout=subprocess.DEVNULL)  # noqa: S603
        manifest = json.loads(manifest_file.read_text())
        ordered = ordered_tables(manifest)
        names = [manifest["nodes"][key]["name"] for key in ordered]
        command = [
            *dbt_command("compile", directory, target), *variables, "--select", *names, PERMISSIONS,
            f"path:{TEST_PATH.rstrip('/')}",
        ]
        subprocess.run(command, check=True, stdout=subprocess.DEVNULL)  # noqa: S603
        return json.loads(manifest_file.read_text()), ordered


def compiled_sql(node, references):
    sql = node["compiled_code"].strip().rstrip(";")
    for relation, replacement in references.items():
        sql = sql.replace(relation, f"`{replacement}`")
    return sql


def materialize(client, prefix, report, *, target="staging"):
    manifest, ordered = compile_graph(target)
    nodes = manifest["nodes"]
    sheet = f"{prefix}.filtro_email"
    load_sheet(client, sheet, report)
    references = {
        manifest["sources"]["source.queries.dashboard_acolherio.filtro_email_dev"]["relation_name"]: sheet,
        nodes["model.queries.raw_sheets_filtro_email_prontuario"]["relation_name"]: sheet,
    }
    for key in ordered:
        node = nodes[key]
        name = node["name"]
        relation = f"{prefix}.{name}"
        phase = "projecao_cartoes" if name == PUBLISHED_MART else name
        execute(client, f"CREATE TABLE `{relation}` AS\n{compiled_sql(node, references)}",
                report, phase)
        if name == ATTENDANCE_SELECTION:
            count = client.get_table(relation).num_rows
            if not count:
                raise RuntimeError("A seleção dbt de atendimentos está vazia")
            report["origem"] = {"modelo": name, "linhas": count}
        references[node["relation_name"]] = relation
    return nodes, references


def schema_signature(schema):
    return [(f.name, f.field_type, f.mode, schema_signature(f.fields)) for f in schema]


def array_counts(fields, parent="t", prefix="", depth=0):
    """Conta cada array no seu próprio grão, inclusive dentro de outros arrays."""
    counts = {}
    for field in fields:
        path = f"{prefix}{field.name}"
        expression = f"{parent}.`{field.name}`"
        if field.mode == "REPEATED":
            counts[path] = f"COALESCE(ARRAY_LENGTH({expression}), 0)"
            alias = f"item{depth}"
            nested = array_counts(field.fields, alias, f"{path}.", depth + 1)
            counts.update({
                key: f"(SELECT COALESCE(SUM({value}), 0) FROM UNNEST({expression}) {alias})"
                for key, value in nested.items()
            })
        else:
            counts.update(array_counts(field.fields, expression, f"{path}.", depth))
    return counts


def compare_volumes(client, candidate, schema, report, *, destination=DESTINATION):
    arrays = array_counts(schema)
    columns = ", ".join(f"SUM(n{i}) AS n{i}" for i in range(len(arrays)))
    measures = ", ".join(f"{value} AS n{i}" for i, value in enumerate(arrays.values()))
    diffs = ", ".join(
        f"STRUCT('{path}' AS campo, SUM(COALESCE(a.n{i}, 0)) AS publicada, "
        f"SUM(COALESCE(b.n{i}, 0)) AS candidata, "
        f"COUNTIF(COALESCE(a.n{i}, 0) != COALESCE(b.n{i}, 0)) AS chaves_divergentes)"
        for i, path in enumerate(arrays)
    )
    row = next(iter(execute(client, f"""
        WITH a AS (
            SELECT id_usuario_unidade, {columns}
            FROM (SELECT id_usuario_unidade, {measures} FROM `{destination}` t)
            GROUP BY id_usuario_unidade
        ), b AS (
            SELECT id_usuario_unidade, {columns}
            FROM (SELECT id_usuario_unidade, {measures} FROM `{candidate}` t)
            GROUP BY id_usuario_unidade
        )
        SELECT COUNTIF(a.id_usuario_unidade IS NULL) AS novas,
               COUNTIF(b.id_usuario_unidade IS NULL) AS removidas,
               [{diffs}] AS colecoes
        FROM a FULL JOIN b USING (id_usuario_unidade)
    """, report, "comparacao_volumes")))
    result = dict(row)
    result["colecoes"] = [dict(collection) for collection in row.colecoes]
    return result


def validate(client, prefix, published, candidate, nodes, references, report):
    if not candidate.num_rows or (published and schema_signature(published.schema) != schema_signature(candidate.schema)):
        raise ValueError("Candidata vazia ou com esquema diferente do destino")
    tests = [
        node for node in nodes.values()
        if node["resource_type"] == "test" and node["original_file_path"].startswith(TEST_PATH)
    ]
    if not tests:
        raise RuntimeError("Testes dbt do prontuário não encontrados")
    checks = report.setdefault("controles", {})
    for test in sorted(tests, key=lambda node: node["name"]):
        name = test["name"]
        sql = compiled_sql(test, references)
        checks[name] = next(iter(execute(client, f"SELECT COUNT(*) AS n FROM ({sql})", report, name))).n

    usuarios = nodes["model.queries.raw_usuarios"]["relation_name"]
    permissoes = compiled_sql(nodes[f"model.queries.{PERMISSIONS}"], references)
    row = next(iter(execute(client, f"""
        WITH permissao AS ({permissoes}), dados AS (
            SELECT DISTINCT CAST(id_paciente AS STRING) AS id_usuario
            FROM {usuarios}
            WHERE UPPER(TRIM(nome)) NOT LIKE 'TESTE%'
        ), publicados AS (
            SELECT DISTINCT id_usuario FROM `{prefix}.{MART}`
        )
        SELECT
            (SELECT COUNT(*) FROM dados LEFT JOIN publicados USING (id_usuario)
                WHERE publicados.id_usuario IS NULL) AS cadastros_ausentes,
            (SELECT COUNT(*) FROM publicados LEFT JOIN dados USING (id_usuario)
                WHERE dados.id_usuario IS NULL) AS cadastros_inesperados,
            COUNT(*) - COUNT(DISTINCT t.id_usuario_unidade) AS chaves_duplicadas_ou_nulas,
            COUNTIF(t.email_filtro IS DISTINCT FROM COALESCE(p.email_filtro, '')) AS emails_divergentes
        FROM `{prefix}.{MART}` t
        LEFT JOIN permissao p ON t.id_unidade = CAST(p.id_unidade AS STRING)
    """, report, "cadastros_chaves_emails")))
    checks.update(dict(row))
    checks["atendimentos_selecionados"] = reconcile_attendance(client, prefix, nodes, report)
    if any(checks.values()):
        raise ValueError(f"Candidata diverge das fontes: {checks}")


def reconcile_attendance(client, prefix, nodes, report):
    usuarios = nodes["model.queries.raw_atendimentos_usuarios"]["relation_name"]
    familias = nodes["model.queries.raw_atendimentos_familias"]["relation_name"]
    row = next(iter(execute(client, f"""
        WITH eventos AS (
            SELECT id_atendimento_modulo, LOGICAL_AND(COALESCE(flag_cancelado, '') = 'S') AS cancelado
            FROM (
                SELECT id_atendimento_modulo, flag_cancelado FROM {usuarios}
                UNION ALL
                SELECT id_atendimento_modulo, flag_cancelado FROM {familias}
            ) GROUP BY 1
        ), a AS (
            SELECT CAST(p.id_usuario AS STRING) AS usuario,
                CONCAT(CAST(p.seq_atendimento AS STRING), p.modulo) AS evento,
                COALESCE(CAST(p.profissional_id AS STRING), '') AS profissional,
                COALESCE(CAST(p.id_atendimento AS STRING), '') AS legado, COUNT(*) AS n
            FROM `{prefix}.{ATTENDANCE_SELECTION}` p
            JOIN `{prefix}.dim_usuarios` d ON p.id_usuario = d.id_usuario
            LEFT JOIN eventos e ON CONCAT(CAST(p.seq_atendimento AS STRING), p.modulo) = e.id_atendimento_modulo
            WHERE NOT COALESCE(e.cancelado, FALSE)
            GROUP BY 1, 2, 3, 4
        ), b AS (
            SELECT t.id_usuario AS usuario, a.id_atendimento_modulo AS evento,
                COALESCE(CAST(p.id_profissional AS STRING), '') AS profissional,
                COALESCE(p.id_atendimento_legado, '') AS legado, COUNT(*) AS n
            FROM `{prefix}.{MART}` t, UNNEST(t.atendimentos.registros) a, UNNEST(a.profissionais) p
            GROUP BY 1, 2, 3, 4
        )
        SELECT COUNTIF(COALESCE(a.n, 0) != COALESCE(b.n, 0)) AS n
        FROM a FULL JOIN b USING (usuario, evento, profissional, legado)
    """, report, "reconciliacao_atendimentos")))
    return row.n


def rows_differ(left, right):
    return f"""
        SELECT 1 FROM (
            SELECT id_usuario_unidade, SHA256(TO_JSON_STRING(t)) AS assinatura, 1 AS multiplicidade
            FROM `{left}` t
            UNION ALL
            SELECT id_usuario_unidade, SHA256(TO_JSON_STRING(t)) AS assinatura, -1 AS multiplicidade
            FROM `{right}` t
        )
        GROUP BY id_usuario_unidade, assinatura
        HAVING SUM(multiplicidade) != 0
    """


def validate_publication(client, prefix, current, report):
    source = f"{prefix}.{MART}"
    destination = f"{prefix}.{PUBLISHED_MART}"
    candidate = client.get_table(destination)
    if not candidate.num_rows or (current and schema_signature(candidate.schema) != schema_signature(current.schema)):
        raise ValueError("Projeção dos cartões vazia ou com esquema diferente do destino")

    row = next(iter(execute(client, f"""
        WITH esperados AS (
            SELECT p.id_usuario_unidade, p.nome_unidade, p.email_filtro,
                (SELECT AS STRUCT a.* REPLACE (
                    ARRAY(
                        SELECT AS STRUCT prof.* REPLACE (a.cadastrante AS id_profissional)
                        FROM UNNEST(a.profissionais) prof WITH OFFSET AS posicao
                        ORDER BY posicao
                    ) AS profissionais
                )) AS evento,
                ARRAY(
                    SELECT DISTINCT TRIM(prof.id_familia_referencia)
                    FROM UNNEST(a.profissionais) prof
                    WHERE NULLIF(TRIM(prof.id_familia_referencia), '') IS NOT NULL
                    ORDER BY 1
                ) AS familias
            FROM `{source}` p CROSS JOIN UNNEST(p.atendimentos.registros) a
        ), obtidos AS (
            SELECT p.id_usuario_unidade, p.nome_unidade, p.email_filtro, a AS evento,
                ARRAY(
                    SELECT DISTINCT f.id_familia FROM UNNEST(p.familias) f ORDER BY 1
                ) AS familias
            FROM `{destination}` p CROSS JOIN UNNEST(p.atendimentos.registros) a
        ), diferencas AS (
            SELECT SHA256(TO_JSON_STRING(e)) AS assinatura, 1 AS sinal FROM esperados e
            UNION ALL
            SELECT SHA256(TO_JSON_STRING(o)) AS assinatura, -1 AS sinal FROM obtidos o
        )
        SELECT COUNT(*) AS n
        FROM (
            SELECT assinatura FROM diferencas GROUP BY assinatura HAVING SUM(sinal) != 0
        )
    """, report, "validacao_cartoes")))
    report["cartoes"] = {"eventos_divergentes": row.n}
    if row.n:
        raise ValueError(f"Projeção dos cartões diverge dos atendimentos validados: {row.n}")
    return candidate


def finish_publication(client, prefix, report, *, destination=DESTINATION):
    relation = f"{prefix}.{PUBLISHED_MART}"
    try:
        candidate = client.get_table(relation)
    except NotFound:
        # A limpeza só ocorre depois da verificação, inclusive em retries.
        try:
            client.get_dataset(prefix)
        except NotFound:
            report["resultado"] = "publicação já verificada e candidata removida"
            return
        # Retomada de publicações iniciadas quando a mart era promovida diretamente.
        relation = f"{prefix}.{MART}"
        candidate = client.get_table(relation)
    target = client.get_table(destination)
    if (target.num_rows != candidate.num_rows
            or schema_signature(target.schema) != schema_signature(candidate.schema)):
        raise RuntimeError("Contagem ou esquema do destino diverge da candidata após a publicação")
    row = next(iter(execute(client, f"""
        SELECT COUNT(*) AS n FROM ({rows_differ(destination, relation)})
    """, report, "verificacao_destino")))
    if row.n:
        raise RuntimeError("Conteúdo do destino diverge da candidata após a publicação")
    client.delete_dataset(prefix, delete_contents=True, not_found_ok=True)
    report["resultado"] = "publicada e verificada"
    report["linhas_publicadas"] = target.num_rows


def refresh_prontuario(run_id, *, publish, report, target="staging"):
    project = {"staging": PROJECT, "prod": PROD_PROJECT}[target]
    destination = f"{project}.abordagem.prontuario_eletronico"
    run_hex = UUID(str(run_id)).hex
    prefix = f"{project}.prontuario_build_{run_hex}"
    job_id = f"prontuario_publish_{run_hex}"
    client = bigquery.Client(project=project, location=LOCATION)
    report["dataset"] = prefix
    if publish:
        try:
            existing = client.get_job(job_id, location=LOCATION)
        except NotFound:
            existing = None
        if existing is not None:
            report.setdefault("jobs", {})["publicacao"] = existing.job_id
            existing.result()
            finish_publication(client, prefix, report, destination=destination)
            return

    dataset = bigquery.Dataset(prefix)
    dataset.location = LOCATION
    dataset.default_table_expiration_ms = 24 * 60 * 60 * 1000
    client.create_dataset(dataset)
    nodes, references = materialize(client, prefix, report, target=target)
    try:
        current = client.get_table(destination)
    except NotFound:
        if target != "prod":
            raise
        current = None
    candidate = client.get_table(f"{prefix}.{MART}")
    validate(client, prefix, current, candidate, nodes, references, report)
    candidate = validate_publication(client, prefix, current or candidate, report)
    report["linhas"] = {"publicada": current.num_rows if current else 0, "candidata": candidate.num_rows}
    if current:
        report["volumes"] = compare_volumes(
            client, f"{prefix}.{PUBLISHED_MART}", candidate.schema, report, destination=destination,
        )
    if not publish:
        report["resultado"] = "candidata validada sem promoção"
        return

    promote(client, prefix, current, run_hex, job_id, report, destination=destination)
    finish_publication(client, prefix, report, destination=destination)


def promote(client, prefix, current, run_hex, job_id, report, *, destination=DESTINATION):
    if current is None:
        execute(client, f"""
            CREATE TABLE `{destination}` AS SELECT * FROM `{prefix}.{PUBLISHED_MART}`
        """, report, "publicacao", job_id=job_id)
        return

    backup = f"{destination}_backup_{run_hex}"
    report["snapshot"] = backup
    execute(client, f"""
        CREATE SNAPSHOT TABLE `{backup}` CLONE `{destination}`
        OPTIONS (expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 7 DAY))
    """, report, "snapshot")
    if client.get_table(destination).etag != current.etag:
        raise RuntimeError("O destino mudou durante a validação")
    execute(client, f"""
        BEGIN TRANSACTION;
        IF EXISTS ({rows_differ(destination, backup)}) THEN
            RAISE USING MESSAGE = 'O destino mudou após o snapshot';
        END IF;
        TRUNCATE TABLE `{destination}`;
        INSERT INTO `{destination}` SELECT * FROM `{prefix}.{PUBLISHED_MART}`;
        COMMIT TRANSACTION;
    """, report, "publicacao", job_id=job_id)
