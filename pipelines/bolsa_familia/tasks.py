# pipeline/tasks.py
import shutil
from datetime import UTC, datetime
from pathlib import Path
from typing import List
from zipfile import ZipFile
from uuid import uuid4

from google.cloud import bigquery
from google.cloud import storage
from google.cloud.storage.blob import Blob
import pandas as pd
import prefect
from prefect import task

from pipelines.bolsa_familia.utils import parse_partition
from pipelines.bolsa_familia.constants import settings


@task
def identify_pending_files(
    bucket_name: str,
    raw_prefix: str,
    dataset_id: str,
    table_id: str
) -> List[Blob]:
    """
    Identifica quais arquivos do Raw ainda não foram carregados no BigQuery.
    """
    logger = prefect.get_run_logger()
    
    # 1. Listar arquivos Raw no GCS
    logger.info(f"Listing raw files from gs://{bucket_name}/{raw_prefix}")
    client_storage = storage.Client(project=settings.GCP_PROJECT)
    bucket = client_storage.bucket(bucket_name)
    blobs = list(bucket.list_blobs(prefix=raw_prefix))
    zip_blobs = [blob for blob in blobs if blob.name.lower().endswith(".zip")]
    
    # 2. Listar partições existentes no BigQuery
    client_bq = bigquery.Client(project=settings.GCP_PROJECT)
    table_ref = f"{settings.GCP_PROJECT}.{dataset_id}.{table_id}"
    
    existing_partitions = []
    try:
        # Verifica se a tabela existe
        client_bq.get_table(table_ref)
        
        query = f"SELECT DISTINCT data_particao FROM `{table_ref}`"
        df = client_bq.query(query).result().to_dataframe()
        existing_partitions = df['data_particao'].astype(str).tolist()
        logger.info(f"Found {len(existing_partitions)} existing partitions in {table_ref}")
    except Exception:
        logger.info(f"Table {table_ref} not found or empty. Assuming all files are new.")

    # 3. Filtrar (Diff)
    files_to_process = []
    for blob in zip_blobs:
        try:
            partition_date = parse_partition(blob)
            if partition_date not in existing_partitions:
                files_to_process.append(blob)
            else:
                logger.debug(f"Skipping {blob.name} (partition {partition_date} exists)")
        except ValueError as e:
            logger.warning(f"Skipping {blob.name}: {e}")

    logger.info(f"Found {len(files_to_process)} new files to process.")
    return files_to_process


@task
def process_and_upload_files(
    files: List[Blob],
    bucket_name: str,
) -> str:
    """
    Processa arquivos ZIP e faz upload para uma pasta ÚNICA de Staging (Run ID).
    Retorna o caminho relativo no GCS onde os arquivos foram salvos.
    """
    logger = prefect.get_run_logger()
    
    # Gera um ID único para esta execução para isolamento total
    run_id = str(uuid4())
    
    # Define caminhos
    # Caminho local temporário
    base_work_dir = Path(f"/tmp/bolsa_familia_{run_id}")
    output_directory = base_work_dir / "processed"
    
    # Caminho remoto no GCS (Isolado por Ambiente e Run ID)
    # Ex: staging/bolsa_familia/prod/a1b2-c3d4/...
    env_name = "prod" if "dev" not in settings.GCP_PROJECT else "dev"
    destination_prefix = f"staging/bolsa_familia/{env_name}/{run_id}"

    if base_work_dir.exists():
        shutil.rmtree(base_work_dir)
    base_work_dir.mkdir(parents=True)

    try:
        logger.info(f"Processing {len(files)} files locally...")
        processed_count = 0
        for blob in files:
            _process_single_zip(blob, output_directory)
            processed_count += 1
        
        # Upload para GCS
        client = storage.Client(project=settings.GCP_PROJECT)
        bucket = client.bucket(bucket_name)
        
        files_to_upload = list(output_directory.rglob("*.csv"))
        logger.info(f"Uploading {len(files_to_upload)} CSV files to gs://{bucket_name}/{destination_prefix}")

        for file_path in files_to_upload:
            # Mantém a estrutura de partições relativa (ano=.../mes=...)
            relative_path = file_path.relative_to(output_directory)
            blob_name = f"{destination_prefix}/{relative_path}"
            
            blob = bucket.blob(blob_name)
            blob.upload_from_filename(str(file_path))
        
        logger.info(f"Upload complete. Staging path: {destination_prefix}")
        return destination_prefix

    finally:
        # Limpeza local
        if base_work_dir.exists():
            shutil.rmtree(base_work_dir)


def _process_single_zip(blob: Blob, output_root: Path):
    """
    Helper function to process a single ZIP file.
    """
    logger = prefect.get_run_logger()
    temp_extract_dir = output_root / "temp_extract" / str(uuid4())
    temp_extract_dir.mkdir(parents=True, exist_ok=True)

    try:
        # Download
        local_zip = temp_extract_dir / blob.name.split("/")[-1]
        blob.download_to_filename(str(local_zip))
        
        # Unzip
        with ZipFile(local_zip, "r") as zip_ref:
            zip_ref.extractall(temp_extract_dir)
            
        partition = parse_partition(blob)
        year, month, _ = partition.split("-")

        # Convert/Format
        for extracted_file in temp_extract_dir.glob("*"):
            # Ignora o próprio zip e processa apenas arquivos de dados
            if extracted_file != local_zip and extracted_file.is_file():
                 # Aceita txt, csv, dat
                if extracted_file.suffix.lower() in ['.txt', '.csv', '.dat']:
                    
                    # Estrutura de pasta estilo Hive (opcional, mas bom para organização)
                    partition_dir = (
                        output_root 
                        / f"ano_particao={int(year)}"
                        / f"mes_particao={int(month)}"
                        / f"data_particao={partition}"
                    )
                    partition_dir.mkdir(parents=True, exist_ok=True)
                    
                    final_file = partition_dir / f"{extracted_file.stem}.csv"
                    
                    try:
                        # Leitura "Burra" (Lê linha inteira) para máxima compatibilidade
                        df = pd.read_csv(
                            extracted_file, 
                            sep='\0', 
                            header=None, 
                            names=['linha_bruta'], 
                            dtype=str, 
                            quoting=3, 
                            encoding='utf-8', 
                            on_bad_lines='skip'
                        )
                        
                        df['timestamp_captura'] = datetime.now(tz=UTC)
                        df['data_particao'] = partition
                        
                        df.to_csv(final_file, index=False, encoding='utf-8')
                        
                    except Exception as e:
                        logger.error(f"Failed to process {extracted_file.name}: {e}")

    finally:
        if temp_extract_dir.exists():
            shutil.rmtree(temp_extract_dir)


@task
def load_to_bigquery(
    dataset_id: str,
    table_id: str,
    bucket_name: str,
    source_path: str
) -> None:
    """
    Carrega dados do GCS para o BigQuery.
    CRÍTICO: Lê apenas da pasta source_path (Run ID específico), evitando duplicatas.
    """
    logger = prefect.get_run_logger()
    client = bigquery.Client(project=settings.GCP_PROJECT)
    table_ref = f"{settings.GCP_PROJECT}.{dataset_id}.{table_id}"
    
    # Create Table if not exists
    try:
        client.get_table(table_ref)
    except Exception:
        logger.info(f"Table {table_ref} not found. Creating...")
        schema = [
            bigquery.SchemaField("linha_bruta", "STRING"),
            bigquery.SchemaField("timestamp_captura", "TIMESTAMP"),
            bigquery.SchemaField("data_particao", "DATE"),
        ]
        table = bigquery.Table(table_ref, schema=schema)
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="data_particao"
        )
        client.create_table(table)
        logger.info(f"Created table {table_ref}")

    # Load Job
    # Lê APENAS os arquivos gerados nesta execução
    gcs_uri = f"gs://{bucket_name}/{source_path}/*.csv"
    
    logger.info(f"Loading data from {gcs_uri} ...")

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        write_disposition="WRITE_APPEND", # Append seguro pois a fonte é única e nova
        schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
        autodetect=True 
    )

    try:
        load_job = client.load_table_from_uri(gcs_uri, table_ref, job_config=job_config)
        load_job.result()
        
        destination_table = client.get_table(table_ref)
        logger.info(f"Loaded {load_job.output_rows} rows. Table now has {destination_table.num_rows} rows.")

    except Exception as e:
        logger.error(f"BigQuery Load Failed: {e}")
        raise
