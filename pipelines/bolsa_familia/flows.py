from prefect import flow

from pipelines.bolsa_familia.constants import settings
from pipelines.bolsa_familia.tasks import (
    identify_pending_files,
    process_and_upload_files,
    load_to_bigquery,
)
from pipelines.tasks import run_dbt_models

@flow(name="Bolsa Família | Carga de Arquivos ZIP")
def bolsa_familia_flow(
    dataset_id: str = settings.GCP_DATASET,
    table_id: str = settings.TABLE_ID,
    raw_path: str = settings.RAW_PATH,
    bucket_name: str = settings.GCS_BUCKET,
) -> None:
    
    files_to_process = identify_pending_files(
        bucket_name=bucket_name,
        raw_prefix=raw_path,
        dataset_id=dataset_id,
        table_id=table_id,
    )

    if not files_to_process:
        return

    staging_path = process_and_upload_files(
        files=files_to_process,
        bucket_name=bucket_name,
    )

    load_to_bigquery(
        dataset_id=dataset_id,
        table_id=table_id,
        bucket_name=bucket_name,
        source_path=staging_path,
    )

    run_dbt_models(model_name="folha")

if __name__ == "__main__":
    bolsa_familia_flow()