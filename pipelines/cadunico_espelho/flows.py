from datetime import date

from prefect import flow, get_run_logger

from pipelines.cadunico_espelho.decision import should_export
from pipelines.cadunico_espelho.tasks import (
    fetch_datalake_partition_date,
    fetch_exported_partition,
    fetch_source_location,
    run_export_script,
)


def _date_for_log(value: date | None) -> str | None:
    if value is None:
        return None
    return value.isoformat()


@flow(name="CadÚnico | Espelho cd_cidadao_pac")
def cadunico_espelho_flow() -> None:
    """Exporta o espelho cd_cidadao_pac só quando há partição nova.

    O gatilho, a coluna cd_cidadao_pac.data_particao e a pasta
    cd_cidadao_pac_YYYY-MM-DD usam só identificacao_primeira_pessoa.
    As outras quatro tabelas do script usam cada uma o próprio
    MAX(data_particao): identificacao_controle, contato,
    documento_pessoa e pessoa_deficiencia. Partição nova só nessas
    quatro não dispara a exportação. O LEFT JOIN com
    ic.estado_cadastro = 'Cadastrado' no WHERE permanece no SQL.
    """
    logger = get_run_logger()
    location = fetch_source_location()
    datalake_date = fetch_datalake_partition_date(location)
    exported_date, export_table_exists = fetch_exported_partition(location)
    exportar = should_export(
        datalake_date,
        exported_date,
        export_table_exists=export_table_exists,
    )
    logger.info(
        "datalake_date=%s exported_date=%s decisao=%s",
        _date_for_log(datalake_date),
        _date_for_log(exported_date),
        "exportar" if exportar else "encerrar",
    )
    if not exportar:
        return
    job_id = run_export_script(location)
    logger.info("job_id=%s", job_id)


if __name__ == "__main__":
    cadunico_espelho_flow()
