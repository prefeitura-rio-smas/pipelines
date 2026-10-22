# -*- coding: utf-8 -*-
"""Tasks do espelho cd_cidadao_pac (CadÚnico → dbt → GCS).

Fluxo de custo:
- Detecção de partição nova: só `INFORMATION_SCHEMA.PARTITIONS` (nenhum dado
  lido; piso de cobrança de 10 MB de metadata).
- Exportação (só quando há partição nova, ~mensal): `dbt build` do mart,
  verificação da partição efetivamente materializada (~18 MB) e `EXPORT DATA`.

Idempotência: o marcador GCS (`<prefixo>/.last_export`) só é gravado depois
de o dbt build e a exportação terminarem com sucesso. Falha no meio do
caminho deixa o marcador intacto e o próximo run volta a exportar.
"""

from datetime import date, datetime, timezone
import json
from pathlib import Path

from google.api_core.exceptions import NotFound
from google.cloud import bigquery, storage
from prefect import get_run_logger, task
from prefect_dbt.cli.commands import trigger_dbt_cli_command

from pipelines.cadunico_espelho.constants import (
    DBT_PROFILES_DIR,
    DBT_PROJECT_DIR,
    LATEST_PARTITION_SQL,
    MART_PARTITION_SQL,
    SOURCE_DATASET,
    TRIGGER_TABLE,
    dbt_build_command,
    settings,
)

SQL_PATH = Path(__file__).with_name("export_cd_cidadao_pac.sql")


def _bq_client() -> bigquery.Client:
    return bigquery.Client(project=settings.GCP_PROJECT)


def _storage_client() -> storage.Client:
    return storage.Client(project=settings.GCP_PROJECT)


def _as_date(value: date | datetime | None) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    raise TypeError("Partição retornada não é uma data.")


def export_uri(partition_date: date) -> str:
    """URI (wildcard) do CSV.gz no GCS — contrato do app do prontuário."""
    label = partition_date.isoformat()
    prefix = settings.GCS_PREFIX
    return (
        f"gs://{settings.GCS_BUCKET}/{prefix}/cd_cidadao_pac_{label}/"
        "cd_cidadao_pac_*.csv.gz"
    )


def build_export_sql(partition_date: date) -> str:
    """Renderiza o EXPORT DATA com a tabela de destino e o URI do ambiente."""
    sql = SQL_PATH.read_text(encoding="utf-8")
    sql = sql.replace("{{TARGET_TABLE}}", settings.FULL_TABLE_ID)
    sql = sql.replace("{{EXPORT_URI}}", export_uri(partition_date))
    return sql


@task(cache_policy=None)
def fetch_source_location() -> str:
    """Location do dataset de origem (usada em todas as queries)."""
    client = _bq_client()
    return client.get_dataset(SOURCE_DATASET).location


@task(cache_policy=None)
def fetch_latest_partition(location: str) -> str | None:
    """Partição (YYYYMM) mais recente da tabela-gatilho (`prefeitura`).

    Lê só metadados (`INFORMATION_SCHEMA.PARTITIONS`): nenhum dado lido,
    piso de cobrança de 10 MB de metadata.
    """
    sql = LATEST_PARTITION_SQL.format(dataset=SOURCE_DATASET, table=TRIGGER_TABLE)
    client = _bq_client()
    rows = list(client.query(sql, location=location).result())
    if not rows or rows[0]["partition_id"] is None:
        return None
    partition_id = str(rows[0]["partition_id"])
    get_run_logger().info("particao_origem=%s tabela=%s", partition_id, TRIGGER_TABLE)
    return partition_id


@task(cache_policy=None)
def export_table_exists() -> bool:
    """A tabela materializada de destino existe? (metadados, ~0 B)"""
    try:
        _bq_client().get_table(settings.FULL_TABLE_ID)
    except NotFound:
        return False
    return True


@task(cache_policy=None)
def run_dbt_build() -> str:
    """Roda `dbt build` do mart (o `+` avança os raws)."""
    target = settings.DBT_TARGET
    command = dbt_build_command(target)
    get_run_logger().info("dbt_build command=%s", command)
    trigger_dbt_cli_command(
        command=command,
        project_dir=DBT_PROJECT_DIR,
        profiles_dir=DBT_PROFILES_DIR,
    )
    return command


@task(cache_policy=None)
def fetch_mart_partition(location: str) -> tuple[date | None, int]:
    """Data da partição EFETIVAMENTE materializada no mart + nº de linhas.

    Confirma que o dbt build produziu dados (o macro pina uma data para as 5
    tabelas; se alguma não tiver essa partição, o mart fica vazio) e devolve a
    data exata usada no nome da pasta exportada.
    """
    sql = MART_PARTITION_SQL.format(table=settings.FULL_TABLE_ID)
    client = _bq_client()
    rows = list(client.query(sql, location=location).result())
    if not rows:
        return None, 0
    partition_date = _as_date(rows[0]["data_particao"])
    count = int(rows[0]["n"] or 0)
    get_run_logger().info(
        "mart_materializado data_particao=%s rows=%s",
        partition_date.isoformat() if partition_date else None,
        count,
    )
    return partition_date, count


@task(cache_policy=None)
def read_watermark() -> dict | None:
    """Lê o marcador da última exportação bem-sucedida no GCS.

    Ausente/ilegível → None (o próximo run volta a exportar).
    """
    logger = get_run_logger()
    client = _storage_client()
    blob = client.bucket(settings.GCS_BUCKET).blob(settings.WATERMARK_BLOB)
    try:
        raw = blob.download_as_text()
    except NotFound:
        logger.info(
            "watermark ausente em gs://%s/%s",
            settings.GCS_BUCKET,
            settings.WATERMARK_BLOB,
        )
        return None
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        logger.warning("watermark ilegivel (JSON invalido); tratando como ausente")
        return None
    if not isinstance(payload, dict) or not payload.get("partition_id"):
        logger.warning("watermark sem partition_id; tratando como ausente")
        return None
    return payload


@task(cache_policy=None)
def write_watermark(payload: dict) -> str:
    """Grava o marcador SÓ após dbt build + export terem terminado bem."""
    client = _storage_client()
    blob = client.bucket(settings.GCS_BUCKET).blob(settings.WATERMARK_BLOB)
    blob.upload_from_string(
        json.dumps(payload, sort_keys=True) + "\n", content_type="application/json"
    )
    get_run_logger().info(
        "watermark gravado em gs://%s/%s", settings.GCS_BUCKET, settings.WATERMARK_BLOB
    )
    return f"gs://{settings.GCS_BUCKET}/{settings.WATERMARK_BLOB}"


@task(cache_policy=None)
def run_export(location: str, partition_date: date) -> dict:
    """Exporta o CSV.gz do mart já materializado para o GCS."""
    logger = get_run_logger()
    sql = build_export_sql(partition_date)
    client = _bq_client()
    job = client.query(sql, location=location)
    job.result()
    result = {
        "job_id": job.job_id,
        "bytes_processed": job.total_bytes_processed,
        "uri": export_uri(partition_date),
    }
    logger.info(
        "export_concluido job_id=%s bytes_processed=%s uri=%s",
        job.job_id,
        job.total_bytes_processed,
        result["uri"],
    )
    return result


def watermark_payload(
    partition_id: str,
    partition_date: date,
    rows: int,
    export_result: dict,
) -> dict:
    """Conteúdo do marcador: partição-gatilho + evidência da exportação."""
    return {
        "partition_id": partition_id,
        "data_particao": partition_date.isoformat(),
        "rows": rows,
        "exported_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "job_id": export_result.get("job_id"),
        "uri": export_result.get("uri"),
    }
