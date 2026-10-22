# -*- coding: utf-8 -*-
"""Flow do espelho cd_cidadao_pac (Prontuário Eletrônico).

Gatilho: cron diário (`prefect.yaml`), sem webhook/evento (decisão do Leone,
2026-10-21). Ordem: partição disponível (macro no dbt) → competências já
exportadas no GCS → decisão → `dbt build` do mart → `EXPORT DATA` → estado.

O dbt só roda no caminho de exportação. Falha no dbt ou no export não grava
o estado, e o próximo run reexporta (idempotente). Recarga corretiva no
mesmo mês não reexporta.
"""

from prefect import flow, get_run_logger

from pipelines.cadunico_espelho.constants import MART_MODEL, settings
from pipelines.cadunico_espelho.tasks import (
    fetch_competencias_exportadas,
    fetch_particao_disponivel,
    run_dbt_build,
    run_export,
    state_payload,
    write_state,
)
from pipelines.cadunico_espelho.utils import partition_key, should_export


@flow(name="CadÚnico | Espelho cd_cidadao_pac")
def cadunico_espelho_flow() -> dict:
    """Exporta o espelho cd_cidadao_pac quando há competência nova."""
    logger = get_run_logger()
    target = settings.MODE
    particao = fetch_particao_disponivel(target)
    competencias = fetch_competencias_exportadas()
    exportada = max(competencias) if competencias else None
    exportar = should_export(particao, exportada)
    base = {
        "mode": target,
        "mart": MART_MODEL,
        "table": settings.FULL_TABLE_ID,
        "bucket": settings.GCS_BUCKET,
        "prefix": settings.GCS_PREFIX,
        "data_particao": particao.isoformat(),
        "competencia": partition_key(particao),
        "competencia_exportada": exportada,
    }
    logger.info(
        "mode=%s table=%s bucket=%s prefix=%s competencia=%s "
        "competencia_exportada=%s decisao=%s",
        base["mode"],
        base["table"],
        base["bucket"],
        base["prefix"],
        base["competencia"],
        exportada,
        "exportar" if exportar else "encerrar",
    )
    if not exportar:
        return {
            **base,
            "status": "skipped",
            "rows": None,
            "uri": None,
            "job_id": None,
            "state_uri": None,
        }

    dbt_command = run_dbt_build(target)
    resultado = run_export(particao)
    estado = state_payload(particao, resultado)
    state_uri = write_state(particao, estado)
    logger.info(
        "exported competencia=%s rows=%s uri=%s job_id=%s state=%s",
        base["competencia"],
        resultado["rows"],
        resultado["uri"],
        resultado["job_id"],
        state_uri,
    )
    return {
        **base,
        "status": "exported",
        "dbt_command": dbt_command,
        "rows": resultado["rows"],
        "bytes_processed": resultado["bytes_processed"],
        "uri": resultado["uri"],
        "job_id": resultado["job_id"],
        "state_uri": state_uri,
    }


if __name__ == "__main__":
    cadunico_espelho_flow()
