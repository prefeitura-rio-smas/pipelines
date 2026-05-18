import os
from prefect import flow
from prefect_dbt.cli.commands import trigger_dbt_cli_command

from pipelines.bolsa_familia.constants import settings
from pipelines.bolsa_familia.tasks import (
    identify_pending_files,
    check_staging_gap,
    process_and_upload_files,
    load_to_wap,
    audit_wap,
    promote_wap,
    cleanup_staging,
)


@flow(name="Bolsa Família | WAC - Carga de Arquivos ZIP")
def bolsa_familia_flow() -> None:
    dataset_id = settings.GCP_DATASET
    table_id = settings.TABLE_ID
    wap_table_id = settings.WAP_TABLE_ID
    raw_path = settings.RAW_PATH
    bucket_name = settings.GCS_BUCKET

    # Cenário D: dados no staging mas dbt ainda não processou
    has_gap = check_staging_gap(
        dataset_id=dataset_id,
        table_id=table_id,
    )

    # Identificar arquivos novos no GCS cujas partições
    # ainda não estão no staging nem no mart
    files_to_process = identify_pending_files(
        bucket_name=bucket_name,
        raw_prefix=raw_path,
        dataset_id=dataset_id,
        table_id=table_id,
    )

    if files_to_process:
        # Pipeline WAC completa para dados novos
        staging_path = process_and_upload_files(
            files=files_to_process,
            bucket_name=bucket_name,
        )

        partitions_loaded = load_to_wap(
            dataset_id=dataset_id,
            wap_table_id=wap_table_id,
            bucket_name=bucket_name,
            source_path=staging_path,
        )

        audit_wap(
            dataset_id=dataset_id,
            wap_table_id=wap_table_id,
            partitions=partitions_loaded,
        )

        promote_wap(
            dataset_id=dataset_id,
            table_id=table_id,
            wap_table_id=wap_table_id,
            partitions=partitions_loaded,
        )

        cleanup_staging(
            dataset_id=dataset_id,
            wap_table_id=wap_table_id,
            bucket_name=bucket_name,
            staging_path=staging_path,
        )

    if not files_to_process and not has_gap:
        # Nada a processar e nenhum gap no staging
        return

    # Rodar dbt (após WAC ou para resolver gap do staging)
    dbt_target = os.getenv("MODE", "staging")
    trigger_dbt_cli_command(
        command=f"dbt build --select int_bolsa_familia_parsed+ --target {dbt_target}",
        project_dir="queries",
        profiles_dir="queries",
    )


if __name__ == "__main__":
    bolsa_familia_flow()