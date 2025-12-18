# pipeline/tasks.py
from datetime import UTC, datetime
from pathlib import Path
from typing import List
from zipfile import ZipFile
from uuid import uuid4

from google.cloud import bigquery
import pandas as pd
import prefect
from prefect import task
from prefect_dbt.cli.commands import DbtCoreOperation
from google.cloud.storage.blob import Blob

from pipelines.arcgis.utils import add_timestamp, bq_client, dataset_ref
from pipelines.bolsa_familia.utils import parse_partition, parse_txt_first_line
from pipelines.bolsa_familia.constants import settings


@task
def get_project_id_task() -> str:
    """
    Get the project ID from the environment.
    """
    return settings.GCP_PROJECT


@task
def get_bolsa_familia_raw_files(prefix: str, bucket_name: str) -> List[Blob]:
    """
    List the Bolsa Família files to process from the raw area.
    """
    logger = prefect.get_run_logger()
    logger.info(f"Listing Bolsa Família raw files from bucket {bucket_name} with prefix {prefix}")

    # Import GCS client
    from google.cloud import storage
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    # List blobs in raw area
    blobs = list(bucket.list_blobs(prefix=prefix))
    logger.info(f"Found {len(blobs)} blobs in raw area")

    # Filter ZIP files
    zip_blobs = [blob for blob in blobs if blob.name.lower().endswith(".zip")]
    logger.info(f"Found {len(zip_blobs)} ZIP files to process")

    return zip_blobs


@task
def get_existing_bolsa_familia_partitions(dataset_id: str, table_id: str) -> List[str]:
    """
    List the existing partitions in the staging area for Bolsa Família data.
    """
    logger = prefect.get_run_logger()
    logger.info(f"Listing existing partitions for {dataset_id}.{table_id}")
    
    client = bq_client()
    dataset = dataset_ref()
    
    # Query to get existing partitions
    query = f"""
        SELECT DISTINCT
            data_particao
        FROM `{dataset}.{table_id}`
        ORDER BY data_particao
    """
    try:
        df = client.query(query).result().to_dataframe()
        partitions = df['data_particao'].astype(str).tolist()
        logger.info(f"Found {len(partitions)} existing partitions")
        return partitions
    except Exception as e:
        logger.info(f"No existing partitions found: {str(e)}")
        return []


@task
def process_bolsa_familia_zip_file(blob: Blob, output_directory: str) -> List[Path]:
    """
    Process a single Bolsa Família ZIP file from GCS.
    """
    logger = prefect.get_run_logger()
    logger.info(f"Processing ZIP file: {blob.name}")
    
    # Create temporary directory for file
    temp_directory: Path = Path("/tmp") / str(uuid4())
    temp_directory.mkdir(parents=True, exist_ok=True)
    logger.info(f"Created temporary directory {temp_directory}")

    # Download blob to temporary directory
    fname = str(temp_directory / blob.name.rpartition("/")[-1])
    blob.download_to_filename(fname)
    logger.info(f"Downloaded blob {blob.name} to {fname}")

    # Unzip file
    unzip_output_directory = temp_directory / "output"
    unzip_output_directory.mkdir(parents=True, exist_ok=True)
    with ZipFile(fname, "r") as zip_file:
        zip_file.extractall(unzip_output_directory)
    logger.info(f"Unzipped {fname} to {unzip_output_directory}")

    # List all files (not just TXT)
    all_files = list(unzip_output_directory.glob("*"))
    logger.info(f"Files extracted: {[f.name for f in all_files]}")

    # Process each extracted file
    processed_files = []
    for extracted_file in all_files:
        if extracted_file.suffix.lower() in ['.txt', '.csv', '.dat']:  # Add other relevant extensions
            # Create partition directory based on file partition
            partition = parse_partition(blob)
            year, month, _ = partition.split("-")
            
            partition_directory = (
                Path(output_directory)
                / f"ano_particao={int(year)}"
                / f"mes_particao={int(month)}"
                / f"data_particao={partition}"
            )
            partition_directory.mkdir(parents=True, exist_ok=True)
            
            # Move file to partition directory
            final_file = partition_directory / extracted_file.name
            extracted_file.rename(final_file)
            processed_files.append(final_file)
            logger.info(f"Processed and moved {extracted_file.name} to {final_file}")

    # Clean up temp directory
    import shutil
    shutil.rmtree(temp_directory)
    
    return processed_files


@task
def create_bolsa_familia_table_if_not_exists(
    dataset_id: str,
    table_id: str,
    schema: List[bigquery.SchemaField] = None
) -> bool:
    """
    Create Bolsa Família table in BigQuery if it doesn't exist.
    """
    logger = prefect.get_run_logger()
    logger.info(f"Checking if table {dataset_id}.{table_id} exists")
    
    client = bq_client()
    table_ref = f"{dataset_ref()}.{table_id}"
    
    try:
        table = client.get_table(table_ref)
        logger.info(f"Table {dataset_id}.{table_id} already exists")
        return True
    except:
        logger.info(f"Table {dataset_id}.{table_id} does not exist, creating...")
        
        # Create a default schema if none provided
        if schema is None:
            schema = [
                bigquery.SchemaField("text", "STRING"),
                bigquery.SchemaField("timestamp_captura", "TIMESTAMP"),
                bigquery.SchemaField("ano_particao", "INTEGER"),
                bigquery.SchemaField("mes_particao", "INTEGER"),
                bigquery.SchemaField("data_particao", "DATE"),
            ]
        
        table = bigquery.Table(table_ref, schema=schema)
        client.create_table(table)
        logger.info(f"Table {dataset_id}.{table_id} created successfully")
        return False


@task
def load_bolsa_familia_to_bigquery(
    data_path: str | Path,
    dataset_id: str,
    table_id: str
) -> int:
    """
    Load Bolsa Família data to BigQuery.
    """
    logger = prefect.get_run_logger()
    logger.info(f"Loading data from {data_path} to {dataset_id}.{table_id}")

    client = bq_client()
    table_ref = f"{dataset_ref()}.{table_id}"

    total_rows_loaded = 0

    # Process each partition directory
    data_path = Path(data_path)
    partition_dirs = [d for d in data_path.iterdir() if d.is_dir()]

    for partition_dir in partition_dirs:
        logger.info(f"Processing partition directory: {partition_dir}")

        # Process each file in the partition directory
        files = list(partition_dir.glob("*"))
        for file_path in files:
            if file_path.suffix.lower() in ['.txt', '.csv']:
                logger.info(f"Processing file: {file_path}")

                # Read the file
                if file_path.suffix.lower() == '.txt':
                    # For TXT files, read as text format
                    df = pd.read_csv(file_path, sep='|', header=None, dtype=str, on_bad_lines='skip')  # Adjust separator as needed
                    df.columns = [f"col_{i}" for i in range(len(df.columns))]
                else:  # .csv
                    df = pd.read_csv(file_path, dtype=str)

                # Add timestamp and partition info
                df = add_timestamp(df)

                # Extract partition information from directory name
                parts = partition_dir.parts
                data_particao = None
                for part in parts:
                    if part.startswith("data_particao="):
                        data_particao = part.split("=")[1]
                        break

                if data_particao:
                    df["data_particao"] = data_particao

                # Upload to BigQuery
                if not df.empty:
                    job_config = bigquery.LoadJobConfig(
                        write_disposition="WRITE_APPEND",
                        schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION]
                    )

                    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
                    job.result()  # Wait for job to complete

                    rows_loaded = len(df)
                    total_rows_loaded += rows_loaded
                    logger.info(f"Loaded {rows_loaded} rows from {file_path.name}")

    logger.info(f"Total rows loaded: {total_rows_loaded}")
    return total_rows_loaded


# Diretório do projeto dbt (pasta paralela `queries`)
DBT_PROJECT_DIR = Path(__file__).parent.parent / "../queries"

@task
def run_bolsa_familia_dbt_models(model_name: str = None):
    """
    Executa os modelos do dbt para Bolsa Família.
    Se um model_name for fornecido, executa apenas esse modelo.
    """
    logger = prefect.get_run_logger()

    if model_name is None:
        logger.info("Nenhum modelo dbt para executar.")
        return None

    logger.info(f"🔄 Executando dbt model: {model_name}...")

    dbt_run_op = DbtCoreOperation(
        commands=[f"dbt run --select {model_name}"],
        project_dir=DBT_PROJECT_DIR,
        profiles_dir=DBT_PROJECT_DIR,
    )

    result = dbt_run_op.run()

    logger.info(f"✅ dbt model {model_name} concluído com sucesso.")
    return result