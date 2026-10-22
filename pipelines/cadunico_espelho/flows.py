# -*- coding: utf-8 -*-
"""Flow do espelho cd_cidadao_pac (CadÚnico).

Ordem: detecta a partição-gatilho (`prefeitura`, via INFORMATION_SCHEMA) →
lê o watermark → decide → `dbt build` do mart → verifica a partição
efetivamente materializada → exporta o CSV → grava o watermark.

O dbt só roda no caminho de exportação. Falha no dbt ou no export não
avançam o watermark (o próximo run reexporta, idempotente).

Gatilho: cron diário (`prefect.yaml`), sem webhook/evento (decisão do Leone,
2026-10-21). Recarga corretiva no mesmo mês não reexporta.
"""

from prefect import flow, get_run_logger

from pipelines.cadunico_espelho.constants import TRIGGER_TABLE, settings
from pipelines.cadunico_espelho.decision import should_export
from pipelines.cadunico_espelho.tasks import (
    export_table_exists,
    fetch_latest_partition,
    fetch_mart_partition,
    fetch_source_location,
    read_watermark,
    run_dbt_build,
    run_export,
    watermark_payload,
    write_watermark,
)


@flow(name="CadÚnico | Espelho cd_cidadao_pac")
def cadunico_espelho_flow() -> dict:
    """Exporta o espelho cd_cidadao_pac só quando há partição nova."""
    logger = get_run_logger()
    location = fetch_source_location()
    source_partition = fetch_latest_partition(location)
    watermark = read_watermark()
    exported_partition = watermark.get("partition_id") if watermark else None
    table_exists = export_table_exists()
    exportar = should_export(
        source_partition,
        exported_partition,
        export_table_exists=table_exists,
    )
    base = {
        "mode": settings.MODE,
        "dbt_target": settings.DBT_TARGET,
        "project": settings.GCP_PROJECT,
        "dataset": settings.GCP_DATASET,
        "table": settings.FULL_TABLE_ID,
        "bucket": settings.GCS_BUCKET,
        "prefix": settings.GCS_PREFIX,
        "trigger_table": TRIGGER_TABLE,
        "source_partition": source_partition,
        "exported_partition": exported_partition,
        "export_table_exists": table_exists,
    }
    logger.info(
        "mode=%s dbt_target=%s project=%s dataset=%s table=%s bucket=%s "
        "prefix=%s trigger_table=%s source_partition=%s exported_partition=%s "
        "table_exists=%s decisao=%s",
        base["mode"],
        base["dbt_target"],
        base["project"],
        base["dataset"],
        base["table"],
        base["bucket"],
        base["prefix"],
        base["trigger_table"],
        source_partition,
        exported_partition,
        table_exists,
        "exportar" if exportar else "encerrar",
    )
    if not exportar:
        return {**base, "status": "skipped", "rows": None, "uri": None}

    dbt_command = run_dbt_build()
    partition_date, rows = fetch_mart_partition(location)
    if partition_date is None or rows == 0:
        raise RuntimeError(
            "dbt build nao materializou dados no mart "
            f"({settings.FULL_TABLE_ID}); exportacao abortada sem avancar o watermark"
        )
    if partition_date.strftime("%Y%m") != source_partition:
        raise RuntimeError(
            "particao materializada "
            f"({partition_date.isoformat()}) diverge da particao-gatilho "
            f"({source_partition}); exportacao abortada sem avancar o watermark"
        )

    result = run_export(location, partition_date)
    payload = watermark_payload(source_partition, partition_date, rows, result)
    watermark_written = True
    try:
        write_watermark(payload)
    except Exception:
        watermark_written = False
        logger.exception(
            "exportacao concluida mas falha ao gravar o watermark; "
            "o proximo run reexporta (idempotente)"
        )
    logger.info(
        "exported rows=%s uri=%s watermark_written=%s",
        rows,
        result["uri"],
        watermark_written,
    )
    return {
        **base,
        "status": "exported",
        "rows": rows,
        "data_particao": partition_date.isoformat(),
        "dbt_command": dbt_command,
        "bytes_processed": result["bytes_processed"],
        "job_id": result["job_id"],
        "uri": result["uri"],
        "watermark_written": watermark_written,
    }


if __name__ == "__main__":
    cadunico_espelho_flow()
