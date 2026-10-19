from datetime import date, datetime
from pathlib import Path

from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from prefect import task

from pipelines.cadunico_espelho.constants import (
    DATALAKE_MAX_SQL,
    EXPORTED_MAX_SQL,
    SOURCE_DATASET,
    settings,
)


def _as_date(value: date | datetime | None) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    raise TypeError("Partição retornada não é uma data.")


def _query_partition_date(sql: str, location: str) -> date | None:
    client = bigquery.Client(project=settings.GCP_PROJECT)
    job = client.query(sql, location=location)
    rows = list(job.result())
    if not rows:
        return None
    return _as_date(rows[0][0])


@task(cache_policy=None)
def fetch_source_location() -> str:
    client = bigquery.Client(project=settings.GCP_PROJECT)
    dataset = client.get_dataset(SOURCE_DATASET)
    return dataset.location


@task(cache_policy=None)
def fetch_datalake_partition_date(location: str) -> date | None:
    return _query_partition_date(DATALAKE_MAX_SQL, location)


@task(cache_policy=None)
def fetch_exported_partition(location: str) -> tuple[date | None, bool]:
    try:
        exported_date = _query_partition_date(EXPORTED_MAX_SQL, location)
    except NotFound:
        return None, False
    return exported_date, True


@task(cache_policy=None)
def run_export_script(location: str) -> str:
    sql_path = Path(__file__).with_name("export_cd_cidadao_pac.sql")
    sql = sql_path.read_text(encoding="utf-8")
    client = bigquery.Client(project=settings.GCP_PROJECT)
    job = client.query(sql, location=location)
    job.result()
    return job.job_id
