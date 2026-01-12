from prefect import flow
from google.cloud.storage.blob import Blob

from pipelines.tasks import run_dbt_models

from pipelines.bolsa_familia.tasks import (
    get_bolsa_familia_raw_files,
    get_existing_bolsa_familia_partitions,
    process_bolsa_familia_zip_file,
    create_bolsa_familia_table_if_not_exists,
    load_bolsa_familia_to_bigquery,
    upload_bolsa_familia_processed_to_gcs,
    get_project_id_task
)
from pipelines.bolsa_familia.utils import parse_partition
from pipelines.bolsa_familia.constants import settings


@flow(name="Bolsa Família | Carga de Arquivos ZIP")
def bolsa_familia_flow() -> None:
    """
    Fluxo para carregar dados do Bolsa Família a partir de arquivos ZIP no GCS para o BigQuery.
    """
    # Configuration for Bolsa Família
    job_name = "bolsa_familia"
    dataset_id = settings.GCP_STAGING_DATASET  # Using staging dataset
    table_id = "folha"
    raw_prefix = "raw/bolsa_familia"
    bucket_name = settings.GCS_BUCKET

    # Get project ID
    project_id = get_project_id_task()

    # Get existing partitions to avoid reprocessing
    existing_partitions = get_existing_bolsa_familia_partitions(dataset_id, table_id)
    print(f"Existing partitions: {existing_partitions}")

    # Get raw files to process
    raw_files = get_bolsa_familia_raw_files(raw_prefix, bucket_name)

    # Filter files that have already been processed
    files_to_process = []
    for blob in raw_files:
        partition_date = parse_partition(blob)
        if partition_date in existing_partitions:
            print(f"Skipping {blob.name} because partition {partition_date} already exists.")
        else:
            files_to_process.append(blob)

    # Check if there are files to process
    has_files = len(files_to_process) > 0

    if has_files:
        print(f"Processing {len(files_to_process)} new files...")
        
        # Process each raw file
        output_directory = f"/tmp/{job_name}_processed"
        
        # Ensure output directory is clean before starting (optional but good practice)
        import shutil
        import os
        if os.path.exists(output_directory):
            shutil.rmtree(output_directory)
            
        for blob in files_to_process:
            processed_files = process_bolsa_familia_zip_file(blob, output_directory)

        # Upload processed files to GCS
        upload_bolsa_familia_processed_to_gcs(
            data_path=output_directory,
            bucket_name=bucket_name,
            destination_prefix="staging/bolsa_familia"
        )

        # Create table if it doesn't exist
        table_exists = create_bolsa_familia_table_if_not_exists(dataset_id, table_id)

        # Load processed data to BigQuery
        rows_loaded = load_bolsa_familia_to_bigquery(
            data_path=output_directory,
            dataset_id=dataset_id,
            table_id=table_id
        )

    else:
        print("No new files to process.")

    # Execute DBT models for Bolsa Família - always run to ensure final table is up to date
    run_dbt_models(model_name="folha")
    #run_bolsa_familia_dbt_models(model_name="folha")


if __name__ == "__main__":
    bolsa_familia_flow()