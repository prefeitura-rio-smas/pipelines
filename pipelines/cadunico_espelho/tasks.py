# -*- coding: utf-8 -*-
"""Tasks do espelho cd_cidadao_pac (CadÚnico → BigQuery → GCS).

Fluxo de custo:
- Detecção de partição nova: só `INFORMATION_SCHEMA.PARTITIONS` (nenhum dado
  lido; piso de cobrança de 10 MB de metadata, contra 1,64 GB/dia antes).
- Exportação (só quando há partição nova, ~mensal): resolve a data exata de
  `data_particao` dentro da partição mais recente de cada tabela e passa como
  query parameter, permitindo partition pruning no `CREATE OR REPLACE TABLE`.

Idempotência: o marcador GCS (`<prefixo>/.last_export`) só é gravado depois
de a exportação inteira terminar com sucesso. Falha no meio do caminho deixa
o marcador intacto e o próximo run volta a exportar.
"""

import calendar
from datetime import date, datetime, timezone
import json
from pathlib import Path

from google.api_core.exceptions import NotFound
from google.cloud import bigquery, storage
from prefect import get_run_logger, task

from pipelines.cadunico_espelho.constants import (
    LATEST_PARTITION_SQL,
    PARTITION_DATE_SELECT,
    PARTITION_DATE_SQL,
    SOURCE_DATASET,
    SOURCE_TABLES,
    TRIGGER_TABLE,
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


def partition_bounds(partition_id: str) -> tuple[date, date]:
    """Primeiro e último dia do mês de uma partição YYYYMM."""
    year = int(partition_id[:4])
    month = int(partition_id[4:6])
    last_day = calendar.monthrange(year, month)[1]
    return date(year, month, 1), date(year, month, last_day)


def export_uri(partition_date: date) -> str:
    """URI (wildcard) do CSV.gz no GCS — contrato do app do prontuário."""
    label = partition_date.isoformat()
    prefix = settings.GCS_PREFIX
    return (
        f"gs://{settings.GCS_BUCKET}/{prefix}/cd_cidadao_pac_{label}/"
        "cd_cidadao_pac_*.csv.gz"
    )


def build_export_sql(dates: dict[str, date]) -> str:
    """Renderiza o SQL de exportação com tabela de destino e URI do ambiente."""
    sql = SQL_PATH.read_text(encoding="utf-8")
    sql = sql.replace("{{TARGET_TABLE}}", settings.FULL_TABLE_ID)
    sql = sql.replace("{{EXPORT_URI}}", export_uri(dates[TRIGGER_TABLE]))
    return sql


def export_query_parameters(
    dates: dict[str, date],
) -> list[bigquery.ScalarQueryParameter]:
    """Query parameters (DATE) — um por tabela de origem."""
    return [
        bigquery.ScalarQueryParameter(SOURCE_TABLES[table], "DATE", dates[table])
        for table in SOURCE_TABLES
    ]


@task(cache_policy=None)
def fetch_source_location() -> str:
    """Location do dataset de origem (usada em todas as queries)."""
    client = _bq_client()
    return client.get_dataset(SOURCE_DATASET).location


@task(cache_policy=None)
def fetch_latest_partitions(location: str) -> dict[str, str]:
    """Partição (YYYYMM) mais recente de cada tabela de origem.

    Lê só metadados (`INFORMATION_SCHEMA.PARTITIONS`): nenhum dado lido,
    piso de cobrança de 10 MB de metadata.
    """
    tables = ", ".join(f"'{table}'" for table in SOURCE_TABLES)
    sql = LATEST_PARTITION_SQL.format(dataset=SOURCE_DATASET, tables=tables)
    client = _bq_client()
    rows = client.query(sql, location=location).result()
    partitions = {row["table_name"]: str(row["partition_id"]) for row in rows}
    get_run_logger().info("particoes_origem=%s", partitions)
    return partitions


@task(cache_policy=None)
def resolve_partition_dates(
    partitions: dict[str, str], location: str
) -> dict[str, date]:
    """Data exata de `data_particao` na partição mais recente de cada tabela.

    Roda apenas no caminho de exportação. Cada subquery é restrita ao mês da
    partição (partition pruning), então o custo é a coluna de partição de uma
    partição por tabela — nunca a tabela inteira.
    """
    selects = []
    for table, partition_id in partitions.items():
        if table not in SOURCE_TABLES:
            continue
        first_day, last_day = partition_bounds(partition_id)
        selects.append(
            PARTITION_DATE_SELECT.format(
                dataset=SOURCE_DATASET,
                table=table,
                first_day=first_day.isoformat(),
                last_day=last_day.isoformat(),
            )
        )
    sql = PARTITION_DATE_SQL.format(selects=",".join(selects))
    client = _bq_client()
    rows = list(client.query(sql, location=location).result())
    if not rows:
        return {}
    row = rows[0]
    dates: dict[str, date] = {}
    for table in SOURCE_TABLES:
        if table not in row.keys():
            continue
        value = _as_date(row[table])
        if value is not None:
            dates[table] = value
    get_run_logger().info(
        "datas_particao=%s", {k: v.isoformat() for k, v in dates.items()}
    )
    return dates


@task(cache_policy=None)
def export_table_exists() -> bool:
    """A tabela materializada de destino existe? (chamada de metadados, ~0 B)"""
    try:
        _bq_client().get_table(settings.FULL_TABLE_ID)
    except NotFound:
        return False
    return True


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
    """Grava o marcador SÓ após a exportação ter terminado com sucesso."""
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
def run_export(
    partitions: dict[str, str], dates: dict[str, date], location: str
) -> dict:
    """Materializa a tabela de destino e exporta o CSV.gz para o GCS."""
    logger = get_run_logger()
    missing = [table for table in SOURCE_TABLES if table not in dates]
    if missing:
        raise ValueError(f"Datas de partição ausentes para: {missing}")
    sql = build_export_sql(dates)
    params = export_query_parameters(dates)
    client = _bq_client()
    job = client.query(
        sql,
        job_config=bigquery.QueryJobConfig(query_parameters=params),
        location=location,
    )
    job.result()
    rows = _count_rows(client, location)
    result = {
        "job_id": job.job_id,
        "rows": rows,
        "bytes_processed": job.total_bytes_processed,
        "uri": export_uri(dates[TRIGGER_TABLE]),
        "partitions": partitions,
        "dates": {k: v.isoformat() for k, v in dates.items()},
    }
    logger.info(
        "export_concluido job_id=%s rows=%s bytes_processed=%s uri=%s",
        job.job_id,
        rows,
        job.total_bytes_processed,
        result["uri"],
    )
    return result


def _count_rows(client: bigquery.Client, location: str) -> int:
    """COUNT(*) sem filtro: lido do metadado da tabela (0 B)."""
    sql = f"SELECT COUNT(*) AS n FROM `{settings.FULL_TABLE_ID}`"  # noqa: S608 (tabela vem de constante interna)
    rows = list(client.query(sql, location=location).result())
    return int(rows[0]["n"]) if rows else 0


def watermark_payload(
    partition_id: str, dates: dict[str, date], export_result: dict
) -> dict:
    """Conteúdo do marcador: partição-gatilho + evidência da exportação."""
    return {
        "partition_id": partition_id,
        "data_particao": dates[TRIGGER_TABLE].isoformat(),
        "exported_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "job_id": export_result.get("job_id"),
        "rows": export_result.get("rows"),
        "uri": export_result.get("uri"),
    }
