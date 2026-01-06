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
from prefect.cache_policies import NO_CACHE
from prefect_dbt.cli.commands import DbtCoreOperation
from google.cloud.storage.blob import Blob

from pipelines.arcgis.utils import add_timestamp
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
    client = storage.Client(project=settings.GCP_PROJECT)
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
    
    client = bigquery.Client(project=settings.GCP_PROJECT)
    table_ref = f"{settings.GCP_PROJECT}.{dataset_id}.{table_id}"
    
    # Query to get existing partitions
    query = f"""
        SELECT DISTINCT
            data_particao
        FROM `{table_ref}`
        ORDER BY data_particao
    """
    try:
        df = client.query(query).result().to_dataframe()
        partitions = df['data_particao'].astype(str).tolist()
        logger.info(f"Found {len(partitions)} existing partitions")
        return partitions
    except Exception as e:
        logger.info(f"No existing partitions found (or table does not exist): {str(e)}")
        return []


@task(cache_policy=NO_CACHE)
def process_bolsa_familia_zip_file(blob: Blob, output_directory: str) -> List[Path]:
    """
    Process a single Bolsa Família ZIP file from GCS.
    Extracts, adds timestamp, and saves as CSV ready for GCS upload.
    """
    logger = prefect.get_run_logger()
    logger.info(f"Processing ZIP file: {blob.name}")
    
    # Create temporary directory for file
    temp_directory: Path = Path("/tmp") / str(uuid4())
    temp_directory.mkdir(parents=True, exist_ok=True)

    try:
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

        # List all files
        all_files = list(unzip_output_directory.glob("*"))
        logger.info(f"Files extracted: {[f.name for f in all_files]}")

        processed_files = []
        for extracted_file in all_files:
            # Check for valid extensions (including .dat)
            if extracted_file.suffix.lower() in ['.txt', '.csv', '.dat']:
                
                # Determine partition
                partition = parse_partition(blob)
                year, month, _ = partition.split("-")
                
                # Setup output directory structure for Hive partitioning (still good for organization)
                partition_directory = (
                    Path(output_directory)
                    / f"ano_particao={int(year)}"
                    / f"mes_particao={int(month)}"
                    / f"data_particao={partition}"
                )
                partition_directory.mkdir(parents=True, exist_ok=True)
                
                final_file = partition_directory / f"{extracted_file.stem}.csv" # Convert all to CSV
                
                logger.info(f"Reading {extracted_file.name} to add timestamp, partition date and save as CSV...")

                try:
                    df = pd.read_csv(
                        extracted_file, 
                        sep='\0',  # Virtual separator to read whole line
                        header=None, 
                        names=['linha_bruta'], 
                        dtype=str, 
                        quoting=3, # QUOTE_NONE - treat quotes as regular characters
                        encoding='utf-8', 
                        on_bad_lines='skip'
                    )
                    
                    # Add timestamp column
                    df['timestamp_captura'] = datetime.now(tz=UTC)
                    # Add partition column explicitly to the file
                    df['data_particao'] = partition
                    
                    # Write to CSV (without index, with header)
                    df.to_csv(final_file, index=False, encoding='utf-8')
                    
                    processed_files.append(final_file)
                    logger.info(f"Saved processed file to {final_file}")
                    
                except Exception as e:
                    logger.error(f"Error processing file {extracted_file.name}: {e}")
                    continue

        return processed_files

    finally:
        # Clean up temp directory
        import shutil
        shutil.rmtree(temp_directory)


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
    logger.info(f"Checking if table {dataset_id}.{table_id} exists in project {settings.GCP_PROJECT}")
    
    client = bigquery.Client(project=settings.GCP_PROJECT)
    table_ref = f"{settings.GCP_PROJECT}.{dataset_id}.{table_id}"
    
    try:
        table = client.get_table(table_ref)
        logger.info(f"Table {table_ref} already exists")
        return True
    except:
        logger.info(f"Table {table_ref} does not exist, creating...")
        
        # Create a default schema if none provided
        if schema is None:
            schema = [
                bigquery.SchemaField("linha_bruta", "STRING"),
                bigquery.SchemaField("timestamp_captura", "TIMESTAMP"),
                bigquery.SchemaField("data_particao", "DATE"),
            ]
        
        table = bigquery.Table(table_ref, schema=schema)
        # Partitioning by data_particao is a good practice for this volume
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="data_particao"
        )
        
        client.create_table(table)
        logger.info(f"Table {table_ref} created successfully")
        return False


@task
def load_bolsa_familia_to_bigquery(
    data_path: str | Path,
    dataset_id: str,
    table_id: str
) -> int:
    """
    Load Bolsa Família data to BigQuery directly from GCS.
    """
    logger = prefect.get_run_logger()
    
    bucket_name = settings.GCS_BUCKET
    destination_prefix = "staging/bolsa_familia"
    
    # Target all CSV files in the staging area recursively
    gcs_uri = f"gs://{bucket_name}/{destination_prefix}/*.csv"
    
    logger.info(f"Loading data from GCS URI: {gcs_uri} to {dataset_id}.{table_id}")
    
    client = bigquery.Client(project=settings.GCP_PROJECT)
    table_ref = f"{settings.GCP_PROJECT}.{dataset_id}.{table_id}"
    
    # Configure Load Job
    # No Hive Partitioning needed because 'data_particao' is now inside the CSV file.
    # BigQuery will use the table's TimePartitioning definition to organize data automatically.
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1, # Skip header
        write_disposition="WRITE_APPEND",
        schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
        autodetect=True 
    )

    try:
        load_job = client.load_table_from_uri(
            gcs_uri,
            table_ref,
            job_config=job_config
        )

        logger.info(f"Starting load job {load_job.job_id}")
        load_job.result()  # Waits for the job to complete.
        
        destination_table = client.get_table(table_ref)
        logger.info(f"Loaded {destination_table.num_rows} rows to {table_ref}")
        return destination_table.num_rows

    except Exception as e:
        logger.error(f"Error loading to BigQuery: {e}")
        raise


@task
def upload_bolsa_familia_processed_to_gcs(
    data_path: str | Path,
    bucket_name: str,
    destination_prefix: str = "staging/bolsa_familia"
) -> int:
    """
    Uploads processed files from local directory to GCS bucket.
    """
    logger = prefect.get_run_logger()
    logger.info(f"Uploading processed files from {data_path} to gs://{bucket_name}/{destination_prefix}")
    
    from google.cloud import storage
    client = storage.Client(project=settings.GCP_PROJECT)
    bucket = client.bucket(bucket_name)
    
    data_path = Path(data_path)
    
    # Find all CSV files recursively
    files_to_upload = list(data_path.rglob("*.csv"))
    
    logger.info(f"Found {len(files_to_upload)} files to upload")
    
    uploaded_count = 0
    for file_path in files_to_upload:
        # Calculate destination path relative to the processing root
        # Structure: ano_particao=.../mes_particao=.../data_particao=.../file.csv
        relative_path = file_path.relative_to(data_path)
        blob_name = f"{destination_prefix}/{relative_path}"
        
        blob = bucket.blob(blob_name)
        blob.upload_from_filename(str(file_path))
        
        logger.info(f"Uploaded {file_path.name} to gs://{bucket_name}/{blob_name}")
        uploaded_count += 1
        
    return uploaded_count


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