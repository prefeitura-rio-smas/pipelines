# -*- coding: utf-8 -*-
"""Flow do espelho cd_cidadao_pac (CadÚnico).

Gatilho: cron diário (`prefect.yaml`), sem webhook/evento (decisão do Leone,
2026-10-21). Só exporta quando aparece uma partição nova na tabela
`identificacao_primeira_pessoa`.

O docstring do SQL lista as 5 tabelas de origem; o gatilho e o nome da pasta
exportada usam `identificacao_primeira_pessoa`. As outras quatro usam cada uma
a própria data de `data_particao` (resolvida dentro da partição mais recente).
Partição nova só nessas quatro não dispara a exportação — comportamento
mantido do PR original (#154).
"""

from prefect import flow, get_run_logger

from pipelines.cadunico_espelho.constants import TRIGGER_TABLE, settings
from pipelines.cadunico_espelho.decision import should_export
from pipelines.cadunico_espelho.tasks import (
    export_table_exists,
    fetch_latest_partitions,
    fetch_source_location,
    read_watermark,
    resolve_partition_dates,
    run_export,
    watermark_payload,
    write_watermark,
)


@flow(name="CadÚnico | Espelho cd_cidadao_pac")
def cadunico_espelho_flow() -> dict:
    """Exporta o espelho cd_cidadao_pac só quando há partição nova."""
    logger = get_run_logger()
    location = fetch_source_location()
    partitions = fetch_latest_partitions(location)
    source_partition = partitions.get(TRIGGER_TABLE)
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
        "project": settings.GCP_PROJECT,
        "dataset": settings.GCP_DATASET,
        "table": settings.FULL_TABLE_ID,
        "bucket": settings.GCS_BUCKET,
        "prefix": settings.GCS_PREFIX,
        "source_partition": source_partition,
        "exported_partition": exported_partition,
        "export_table_exists": table_exists,
    }
    logger.info(
        "mode=%s project=%s dataset=%s table=%s bucket=%s prefix=%s "
        "source_partition=%s exported_partition=%s table_exists=%s decisao=%s",
        base["mode"],
        base["project"],
        base["dataset"],
        base["table"],
        base["bucket"],
        base["prefix"],
        source_partition,
        exported_partition,
        table_exists,
        "exportar" if exportar else "encerrar",
    )
    if not exportar:
        return {**base, "status": "skipped", "rows": None, "uri": None}

    dates = resolve_partition_dates(partitions, location)
    result = run_export(partitions, dates, location)
    payload = watermark_payload(source_partition, dates, result)
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
        result["rows"],
        result["uri"],
        watermark_written,
    )
    return {
        **base,
        "status": "exported",
        "rows": result["rows"],
        "bytes_processed": result["bytes_processed"],
        "job_id": result["job_id"],
        "uri": result["uri"],
        "partitions": result["partitions"],
        "dates": result["dates"],
        "watermark_written": watermark_written,
    }


if __name__ == "__main__":
    cadunico_espelho_flow()
