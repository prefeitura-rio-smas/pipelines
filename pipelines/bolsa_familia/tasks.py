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


@task(cache_policy=None)
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
    
    # 2. Listar partições existentes no BigQuery (na tabela FINAL de Mart)
    client_bq = bigquery.Client(project=settings.GCP_PROJECT)
    # Fonte da verdade para idempotência: Tabela Final
    mart_dataset = "bolsa_familia" if "dev" not in settings.GCP_PROJECT else "gerenciamento__dbt"
    table_ref = f"{settings.GCP_PROJECT}.{mart_dataset}.{table_id}"
    
    existing_partitions = []
    try:
        # Verifica se a tabela existe
        client_bq.get_table(table_ref)
        
        query = f"SELECT DISTINCT data_particao FROM `{table_ref}`"
        df = client_bq.query(query).result().to_dataframe()
        existing_partitions = df['data_particao'].astype(str).tolist()
        logger.info(f"Found {len(existing_partitions)} existing partitions in FINAL table {table_ref}")
    except Exception:
        logger.info(f"Final table {table_ref} not found or empty. Assuming all files are new.")

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


@task(cache_policy=None)
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
    CRÍTICO: Usa WRITE_TRUNCATE na partição específica para garantir idempotência.
    """
    logger = prefect.get_run_logger()
    client = bigquery.Client(project=settings.GCP_PROJECT)
    
    # Extrair a data da partição a partir do source_path (staging/bolsa_familia/env/run_id/ano=.../mes=.../data=...)
    # O source_path termina na pasta do run_id. Os arquivos dentro têm a estrutura de pastas Hive.
    # Vamos listar os arquivos para descobrir as partições que precisam ser carregadas.
    storage_client = storage.Client(project=settings.GCP_PROJECT)
    bucket = storage_client.bucket(bucket_name)
    blobs = list(bucket.list_blobs(prefix=source_path))
    
    # Mapear arquivos para suas partições para fazer loads individuais por partição (necessário para TRUNCATE de partição)
    partitions_found = {}
    for blob in blobs:
        if blob.name.endswith(".csv") and "data_particao=" in blob.name:
            part = blob.name.split("data_particao=")[1].split("/")[0]
            if part not in partitions_found:
                partitions_found[part] = []
            partitions_found[part].append(blob.name)

    for partition_date, files in partitions_found.items():
        # Formato da tabela com partição para TRUNCATE: dataset.tabela$YYYYMMDD
        partition_suffix = partition_date.replace("-", "")
        table_ref = f"{settings.GCP_PROJECT}.{dataset_id}.{table_id}${partition_suffix}"
        
        # Create Main Table if not exists (sem o sufixo $)
        main_table_ref = f"{settings.GCP_PROJECT}.{dataset_id}.{table_id}"
        try:
            client.get_table(main_table_ref)
        except Exception:
            logger.info(f"Main table {main_table_ref} not found. Creating...")
            schema = [
                bigquery.SchemaField("linha_bruta", "STRING"),
                bigquery.SchemaField("timestamp_captura", "TIMESTAMP"),
                bigquery.SchemaField("data_particao", "DATE"),
            ]
            table = bigquery.Table(main_table_ref, schema=schema)
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="data_particao"
            )
            client.create_table(table)

        # Load Job para a PARTIÇÃO específica
        # Como o bucket.list_blobs já filtrou por source_path (Run ID), estamos seguros
        gcs_uris = [f"gs://{bucket_name}/{f}" for f in files]
        
        logger.info(f"Loading {len(gcs_uris)} files into partition {partition_date} ({table_ref}) ...")

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            write_disposition="WRITE_TRUNCATE", # Idempotência: Limpa a partição antes de inserir
            schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
            autodetect=True 
        )

        try:
            load_job = client.load_table_from_uris(gcs_uris, table_ref, job_config=job_config)
            load_job.result()
            logger.info(f"Loaded {load_job.output_rows} rows into partition {partition_date}.")
        except Exception as e:
            logger.error(f"BigQuery Load Failed for partition {partition_date}: {e}")
            raise
