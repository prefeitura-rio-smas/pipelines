"""Materializa e valida o prontuário em BigQuery desenvolvimento."""
import argparse
import csv
import io
import json
from pathlib import Path
import shutil
import subprocess
import sys
from tempfile import TemporaryDirectory
from urllib.parse import urlsplit
from urllib.request import urlopen
from uuid import uuid4

from google.cloud import bigquery

PROJECT = "rj-smas-dev"
LOCATION = "US"
DESTINATION = f"{PROJECT}.abordagem.prontuario_eletronico"
SOURCE = f"{PROJECT}.dashboard_acolherio.filtro_email_dev"
MODELS = ("int_registros_centro_pop", "int_centro_pop_mensal", "prontuario_eletronico")
QUERIES = Path(__file__).resolve().parents[1]


def execute(client, sql, config=None):
    job = client.query(sql, job_config=config, location=LOCATION)
    print(f"Job {job.job_id}", flush=True)
    try:
        return job.result()
    except Exception as exc:
        message = job.error_result or {}
        raise RuntimeError(f"Job {job.job_id}: {message.get('message', exc)}") from None


def load_sheet(client, destination):
    uri, = client.get_table(SOURCE).external_data_configuration.source_uris
    parsed = urlsplit(uri)
    parts = parsed.path.split("/")
    if (parsed.hostname != "docs.google.com" or len(parts) < 4
            or parts[:3] != ["", "spreadsheets", "d"] or not parts[3]):
        raise ValueError("Origem inesperada da planilha")
    url = f"https://docs.google.com/spreadsheets/d/{parts[3]}/export?format=csv&gid=0"
    with urlopen(url, timeout=30) as response:
        csv_text = response.read().decode("utf-8-sig")
    rows = [row for row in csv.reader(io.StringIO(csv_text), strict=True) if any(row)]
    if not rows or any(len(row) != 2 or not row[0].strip() for row in rows):
        raise ValueError("A planilha deve conter unidade e e-mail, sem cabeçalho")
    records = [dict(unidade_atendimento=unit, email=email) for unit, email in rows]
    config = bigquery.LoadJobConfig(schema=[
        bigquery.SchemaField("unidade_atendimento", "STRING"),
        bigquery.SchemaField("email", "STRING"),
    ])
    job = client.load_table_from_json(records, destination, job_config=config, location=LOCATION)
    print(f"Planilha: {len(records)} linhas, job {job.job_id}", flush=True)
    job.result()


def compile_models(target):
    dbt = Path(sys.executable).with_name("dbt")
    subprocess.run(
        [str(dbt) if dbt.exists() else shutil.which("dbt") or "dbt",
         "--quiet", "compile", "--project-dir", str(QUERIES), "--profiles-dir", str(QUERIES),
         "--target", "dev", "--target-path", str(target), "--select",
         *MODELS, "int_filtro_email_unidades"],
        check=True, stdout=subprocess.DEVNULL,
    )
    manifest = json.loads((target / "manifest.json").read_text())
    nodes = manifest["nodes"]
    return {name: nodes[f"model.queries.{name}"] for name in (*MODELS, "int_filtro_email_unidades")}


def sql_for(node, references):
    sql = node["compiled_code"].strip().rstrip(";")
    for relation, replacement in references.items():
        sql = sql.replace(relation, f"`{replacement}`")
    return sql


def schema_signature(schema):
    return [(field.name, field.field_type, field.mode, schema_signature(field.fields)) for field in schema]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--publicar", action="store_true", help="Promove a candidata após os controles")
    args = parser.parse_args()
    sha = subprocess.check_output(["git", "-C", str(QUERIES), "rev-parse", "HEAD"], text=True).strip()
    print(f"Commit {sha}", flush=True)
    with TemporaryDirectory(prefix="prontuario-dbt-") as directory:
        nodes = compile_models(Path(directory))

    client = bigquery.Client(project=PROJECT, location=LOCATION)
    prefix = f"{PROJECT}.prontuario_build_{uuid4().hex[:12]}"
    dataset = bigquery.Dataset(prefix)
    dataset.location = LOCATION
    dataset.default_table_expiration_ms = 24 * 60 * 60 * 1000
    client.create_dataset(dataset)
    print(f"Dataset {prefix}", flush=True)
    sheet = f"{prefix}.filtro_email"
    load_sheet(client, sheet)
    references = {"`" + "`.`".join(SOURCE.split(".")) + "`": sheet}
    for name in MODELS:
        node = nodes[name]
        relation = f"{prefix}.{name}"
        print(f"Materializando {name}", flush=True)
        execute(client, f"CREATE TABLE `{relation}` AS\n{sql_for(node, references)}")
        references[node["relation_name"]] = relation

    monthly = f"{prefix}.{MODELS[1]}"
    candidate = f"{prefix}.{MODELS[2]}"
    before = client.get_table(DESTINATION)
    review = client.get_table(candidate)
    if schema_signature(before.schema) != schema_signature(review.schema):
        raise ValueError("A candidata diverge do esquema publicado, incluindo a ordem dos campos")
    if any(field.policy_tags for field in walk_fields(before.schema)):
        raise ValueError("Destino contém policy tags e requer revisão antes da promoção")

    permissions = sql_for(nodes["int_filtro_email_unidades"], references)
    checks = dict(next(iter(execute(client, f"""
        WITH mensal AS (
            SELECT COUNT(*) AS mensal_linhas,
                   COUNT(DISTINCT TO_JSON_STRING(STRUCT(id_usuario,id_unidade,mes_referencia))) AS mensal_chaves,
                   MIN(mes_referencia) AS primeiro_mes, MAX(mes_referencia) AS ultimo_mes,
                   COUNT(DISTINCT mes_referencia) AS competencias,
                   COUNTIF(id_usuario IS NULL OR id_unidade IS NULL OR mes_referencia IS NULL
                       OR quantidade_atendimentos != ARRAY_LENGTH(atendimentos)
                       OR quantidade_evolucoes != ARRAY_LENGTH(evolucoes)
                       OR quantidade_participacoes != (SELECT COUNTIF(o.tipo_registro = 'participacao')
                           FROM UNNEST(ocorrencias) o)
                       OR ARRAY_LENGTH(atendimentos) + ARRAY_LENGTH(evolucoes) + ARRAY_LENGTH(ocorrencias) = 0
                   ) AS erros_mensais
            FROM `{monthly}`
        ), permissao AS ({permissions}),
        chaves AS (
            SELECT id_usuario_unidade, COUNTIF(atual) AS atual, COUNTIF(NOT atual) AS candidata
            FROM (
                SELECT id_usuario_unidade, TRUE AS atual FROM `{DESTINATION}`
                UNION ALL
                SELECT id_usuario_unidade, FALSE FROM `{candidate}`
            )
            GROUP BY id_usuario_unidade
        )
        SELECT mensal.*,
            (SELECT COUNTIF(atual > 0 AND candidata = 0) FROM chaves) AS chaves_removidas,
            (SELECT COUNTIF(atual = 0 AND candidata > 0) FROM chaves) AS chaves_novas,
            (SELECT COUNTIF(candidata > 1 OR (id_usuario_unidade IS NULL AND candidata > 0)) FROM chaves) AS erros_chave,
            (SELECT COUNT(*) FROM `{candidate}` t LEFT JOIN permissao p
                ON t.id_unidade = CAST(p.id_unidade AS STRING)
                WHERE t.email_filtro IS DISTINCT FROM COALESCE(p.email_filtro, '')) AS erros_email
        FROM mensal
    """))))
    print(f"Publicada: {before.num_rows}; candidata: {review.num_rows}; controles: {checks}", flush=True)
    if (not review.num_rows or not checks["mensal_linhas"]
        or checks["mensal_linhas"] != checks["mensal_chaves"]
        or any(checks[key] for key in ("erros_mensais", "erros_chave", "erros_email", "chaves_removidas", "chaves_novas"))):
        raise ValueError("A candidata requer revisão das divergências antes de publicar")
    if args.publicar:
        if client.get_table(DESTINATION).etag != before.etag:
            raise RuntimeError("O destino mudou durante a validação")
        execute(client, f"SELECT * FROM `{candidate}`", bigquery.QueryJobConfig(
            destination=DESTINATION, write_disposition="WRITE_TRUNCATE_DATA",
        ))
        after = client.get_table(DESTINATION)
        if after.num_rows != review.num_rows or schema_signature(after.schema) != schema_signature(review.schema):
            raise RuntimeError("A publicação diverge da candidata validada")
        print(f"Publicado: {DESTINATION}, {after.num_rows} linhas", flush=True)


def walk_fields(schema):
    for field in schema:
        yield field
        yield from walk_fields(field.fields)


if __name__ == "__main__":
    main()
