# -*- coding: utf-8 -*-
"""Tasks do espelho cd_cidadao_pac (CadÚnico → dbt → GCS).

Ordem no flow: partição disponível (macro no dbt) → competências já
exportadas (estados no GCS) → decisão → `dbt build` do mart → `EXPORT DATA` →
estado. O estado (`{prefixo}/_state/cd_cidadao_pac_<data>.json`) só é gravado
depois do export bem-sucedido: falha no dbt ou no export não o grava e o
próximo run reexporta (idempotente, `overwrite=true`).
"""

from datetime import date, datetime, timezone
import json

from google.cloud import bigquery, storage
from prefect import get_run_logger, task
from prefect_dbt.cli.commands import trigger_dbt_cli_command

from pipelines.cadunico_bq_to_prontuario_eletronico.constants import (
    MART_MODEL,
    settings,
)
from pipelines.cadunico_bq_to_prontuario_eletronico.utils import (
    export_uri,
    exported_dates,
    state_blob,
)
from pipelines.utils.dbt import obter_particao_cadunico
from pipelines.utils.export import export_table_to_gcs

_COUNT_SQL = "SELECT COUNT(*) AS n FROM `{table}`"


def _bq_client() -> bigquery.Client:
    return bigquery.Client(project=settings.GCP_PROJECT)


def _storage_client() -> storage.Client:
    return storage.Client(project=settings.GCP_PROJECT)


def dbt_build_command(target: str) -> str:
    """Comando do dbt build do espelho (target = prod|staging)."""
    return f"dbt build --select +{MART_MODEL} --target {target}"


@task(cache_policy=None)
def fetch_particao_disponivel(target: str) -> date:
    """Partição disponível no CadÚnico (macro `obter_particao_cadunico()`)."""
    particao = obter_particao_cadunico(target)
    get_run_logger().info("particao_disponivel=%s", particao.isoformat())
    return particao


@task(cache_policy=None)
def fetch_competencias_exportadas() -> list[str]:
    """Competências (AAAA-MM) já exportadas, a partir dos estados no GCS."""
    competencias = exported_dates(_storage_client(), settings.GCS_PREFIX)
    get_run_logger().info("competencias_exportadas=%s", competencias)
    return competencias


@task(cache_policy=None)
def run_dbt_build(target: str) -> str:
    """Roda `dbt build` do mart (o `+` avança int e raws)."""
    command = dbt_build_command(target)
    get_run_logger().info("dbt_build command=%s", command)
    trigger_dbt_cli_command(
        command=command,
        project_dir="queries",
        profiles_dir="queries",
    )
    return command


@task(cache_policy=None)
def run_export(partition_date: date) -> dict:
    """Exporta o mart já materializado para `{prefixo}/{pasta}/`.

    Conta as linhas ANTES de exportar: mart vazio (partição-gatilho sem dados
    nas fontes) aborta sem publicar CSV e sem gravar estado.
    """
    logger = get_run_logger()
    client = _bq_client()
    rows = _count_rows(client)
    if rows == 0:
        raise RuntimeError(
            f"tabela vazia ({settings.FULL_TABLE_ID}); exportacao abortada"
        )
    uri = export_uri(partition_date)
    result = export_table_to_gcs(client, settings.FULL_TABLE_ID, uri)
    logger.info(
        "export_concluido job_id=%s rows=%s bytes_processed=%s uri=%s",
        result.job_id,
        rows,
        result.bytes_processed,
        result.uri,
    )
    return {
        "job_id": result.job_id,
        "uri": result.uri,
        "bytes_processed": result.bytes_processed,
        "rows": rows,
    }


def _count_rows(client: bigquery.Client) -> int:
    """Linhas da tabela exportada (`COUNT(*)` sem filtro: lido do metadado)."""
    sql = _COUNT_SQL.format(table=settings.FULL_TABLE_ID)
    rows = list(client.query(sql).result())
    return int(rows[0]["n"]) if rows else 0


@task(cache_policy=None)
def write_state(partition_date: date, payload: dict) -> str:
    """Grava o estado da exportação SÓ depois do export bem-sucedido."""
    client = _storage_client()
    blob_name = state_blob(partition_date)
    blob = client.bucket(settings.GCS_BUCKET).blob(blob_name)
    blob.upload_from_string(
        json.dumps(payload, sort_keys=True) + "\n",
        content_type="application/json",
    )
    path = f"gs://{settings.GCS_BUCKET}/{blob_name}"
    get_run_logger().info("estado gravado em %s", path)
    return path


def state_payload(
    partition_date: date,
    export_result: dict,
) -> dict:
    """Conteúdo do estado: evidência da exportação da competência."""
    return {
        "data_particao": partition_date.isoformat(),
        "competencia": partition_date.strftime("%Y-%m"),
        "mode": settings.MODE,
        "table": settings.FULL_TABLE_ID,
        "rows": export_result.get("rows"),
        "uri": export_result.get("uri"),
        "job_id": export_result.get("job_id"),
        "exported_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
