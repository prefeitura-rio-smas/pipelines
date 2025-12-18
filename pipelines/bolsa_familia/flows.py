from prefect import flow, task
from typing import List
from google.cloud.storage.blob import Blob

from pipelines.bolsa_familia.tasks import (
    get_bolsa_familia_raw_files,
    get_existing_bolsa_familia_partitions,
    process_bolsa_familia_zip_file,
    create_bolsa_familia_table_if_not_exists,
    load_bolsa_familia_to_bigquery,
    run_bolsa_familia_dbt_models,
    get_project_id_task
)
from pipelines.bolsa_familia.constants import settings


@flow(name="Bolsa Família | Carga de Arquivos ZIP")
def bolsa_familia_flow() -> None:
    """
    Fluxo para carregar dados do Bolsa Família a partir de arquivos ZIP no GCS para o BigQuery.
    """
    # Configuration for Bolsa Família
    job_name = "bolsa_familia"
    dataset_id = settings.GCP_DATASET
    table_id = "registro_beneficios"
    raw_prefix = "raw/bolsa_familia/registro_beneficios"
    bucket_name = settings.GCS_BUCKET
    batch_size = 10000

    # Get project ID
    project_id = get_project_id_task()

    # Get existing partitions to avoid reprocessing
    existing_partitions = get_existing_bolsa_familia_partitions(dataset_id, table_id)

    # Get raw files to process
    raw_files = get_bolsa_familia_raw_files(raw_prefix, bucket_name)

    # Process each raw file
    output_directory = f"/tmp/{job_name}_processed"
    for blob in raw_files:
        # For now, process all files. In a more sophisticated implementation,
        # we could check if the file's partition already exists
        processed_files = process_bolsa_familia_zip_file(blob, output_directory)

    # Create table if it doesn't exist
    table_exists = create_bolsa_familia_table_if_not_exists(dataset_id, table_id)

    # Load processed data to BigQuery
    rows_loaded = load_bolsa_familia_to_bigquery(
        data_path=output_directory,
        dataset_id=dataset_id,
        table_id=table_id,
        batch_size=batch_size
    )

    # Execute DBT models for Bolsa Família
    run_bolsa_familia_dbt_models(model_name="bolsa_familia")


if __name__ == "__main__":
    bolsa_familia_flow()