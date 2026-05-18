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
    table_id: str,
) -> List[Blob]:
    """
    Identifica ZIPs no GCS cujas partições ainda não existem
    na tabela de staging nem na mart. Verifica ambas para
    evitar reprocessamento desnecessário.
    """
    logger = prefect.get_run_logger()

    client_storage = storage.Client(project=settings.GCP_PROJECT)
    bucket = client_storage.bucket(bucket_name)
    blobs = list(bucket.list_blobs(prefix=raw_prefix))
    zip_blobs = [blob for blob in blobs if blob.name.lower().endswith(".zip")]

    if not zip_blobs:
        logger.info("No ZIP files found in GCS.")
        return []

    existing_partitions = set()
    client_bq = bigquery.Client(project=settings.GCP_PROJECT)

    staging_ref = f"{settings.GCP_PROJECT}.{dataset_id}.{table_id}"
    try:
        client_bq.get_table(staging_ref)
        staging_df = client_bq.query(
            f"SELECT DISTINCT data_particao FROM `{staging_ref}`"
        ).result().to_dataframe()
        staging_partitions = set(staging_df['data_particao'].astype(str).tolist())
        existing_partitions.update(staging_partitions)
        logger.info(f"Found {len(staging_partitions)} partitions in staging {staging_ref}")
    except Exception:
        logger.info(f"Staging table {staging_ref} not found or empty.")

    mart_dataset = "bolsa_familia" if "dev" not in settings.GCP_PROJECT else "gerenciamento__dbt"
    mart_ref = f"{settings.GCP_PROJECT}.{mart_dataset}.{table_id}"
    try:
        client_bq.get_table(mart_ref)
        mart_df = client_bq.query(
            f"SELECT DISTINCT data_particao FROM `{mart_ref}`"
        ).result().to_dataframe()
        mart_partitions = set(mart_df['data_particao'].astype(str).tolist())
        existing_partitions.update(mart_partitions)
        logger.info(f"Found {len(mart_partitions)} partitions in mart {mart_ref}")
    except Exception:
        logger.info(f"Mart table {mart_ref} not found or empty.")

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
def check_staging_gap(dataset_id: str, table_id: str) -> bool:
    """
    Verifica se existem partições na staging que ainda não foram
    processadas pelo dbt (estão no staging mas não no mart).
    Cenário D: o passo anterior completou mas o dbt falhou.
    """
    logger = prefect.get_run_logger()
    client = bigquery.Client(project=settings.GCP_PROJECT)

    staging_ref = f"{settings.GCP_PROJECT}.{dataset_id}.{table_id}"
    staging_partitions = set()
    try:
        client.get_table(staging_ref)
        staging_df = client.query(
            f"SELECT DISTINCT data_particao FROM `{staging_ref}`"
        ).result().to_dataframe()
        staging_partitions = set(staging_df['data_particao'].astype(str).tolist())
    except Exception:
        logger.info("Staging table not found or empty. No gap to fill.")
        return False

    if not staging_partitions:
        return False

    mart_dataset = "bolsa_familia" if "dev" not in settings.GCP_PROJECT else "gerenciamento__dbt"
    mart_ref = f"{settings.GCP_PROJECT}.{mart_dataset}.{table_id}"
    mart_partitions = set()
    try:
        client.get_table(mart_ref)
        mart_df = client.query(
            f"SELECT DISTINCT data_particao FROM `{mart_ref}`"
        ).result().to_dataframe()
        mart_partitions = set(mart_df['data_particao'].astype(str).tolist())
    except Exception:
        pass

    gap = staging_partitions - mart_partitions
    if gap:
        logger.info(f"Found {len(gap)} partitions in staging but not in mart: {sorted(gap)}")
        return True

    logger.info("No staging gap found. All staging partitions are in mart.")
    return False


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

    run_id = str(uuid4())
    base_work_dir = Path(f"/tmp/bolsa_familia_{run_id}")
    output_directory = base_work_dir / "processed"

    env_name = "prod" if "dev" not in settings.GCP_PROJECT else "dev"
    destination_prefix = f"staging/bolsa_familia/{env_name}/{run_id}"

    if base_work_dir.exists():
        shutil.rmtree(base_work_dir)
    base_work_dir.mkdir(parents=True)

    try:
        logger.info(f"Processing {len(files)} files locally...")
        for blob in files:
            _process_single_zip(blob, output_directory)

        client = storage.Client(project=settings.GCP_PROJECT)
        bucket = client.bucket(bucket_name)

        files_to_upload = list(output_directory.rglob("*.csv"))
        logger.info(f"Uploading {len(files_to_upload)} CSV files to gs://{bucket_name}/{destination_prefix}")

        for file_path in files_to_upload:
            relative_path = file_path.relative_to(output_directory)
            blob_name = f"{destination_prefix}/{relative_path}"
            blob = bucket.blob(blob_name)
            blob.upload_from_filename(str(file_path))

        logger.info(f"Upload complete. Staging path: {destination_prefix}")
        return destination_prefix

    finally:
        if base_work_dir.exists():
            shutil.rmtree(base_work_dir)


def _process_single_zip(blob: Blob, output_root: Path):
    """
    Faz o download e processamento de um arquivo ZIP individual.
    """
    logger = prefect.get_run_logger()
    temp_extract_dir = output_root / "temp_extract" / str(uuid4())
    temp_extract_dir.mkdir(parents=True, exist_ok=True)

    try:
        local_zip = temp_extract_dir / blob.name.split("/")[-1]
        blob.download_to_filename(str(local_zip))

        with ZipFile(local_zip, "r") as zip_ref:
            zip_ref.extractall(temp_extract_dir)

        partition = parse_partition(blob)
        year, month, _ = partition.split("-")

        for extracted_file in temp_extract_dir.glob("*"):
            if extracted_file != local_zip and extracted_file.is_file():
                if extracted_file.suffix.lower() in ['.txt', '.csv', '.dat']:
                    partition_dir = (
                        output_root
                        / f"ano_particao={int(year)}"
                        / f"mes_particao={int(month)}"
                        / f"data_particao={partition}"
                    )
                    partition_dir.mkdir(parents=True, exist_ok=True)

                    final_file = partition_dir / f"{extracted_file.stem}.csv"

                    try:
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


@task(cache_policy=None)
def load_to_wap(
    dataset_id: str,
    wap_table_id: str,
    bucket_name: str,
    source_path: str,
) -> List[str]:
    """
    Carrega dados do GCS para a tabela WAP (Write-Audit-Publish).
    Trunca a WAP antes do load para garantir idempotência.
    Retorna a lista de partições carregadas.
    """
    logger = prefect.get_run_logger()
    client = bigquery.Client(project=settings.GCP_PROJECT)
    wap_ref = f"{settings.GCP_PROJECT}.{dataset_id}.{wap_table_id}"

    try:
        client.get_table(wap_ref)
        logger.info(f"Truncating WAP table {wap_ref}")
        client.query(f"TRUNCATE TABLE `{wap_ref}`").result()
    except Exception:
        logger.info(f"WAP table {wap_ref} not found. Creating...")
        schema = [
            bigquery.SchemaField("linha_bruta", "STRING"),
            bigquery.SchemaField("timestamp_captura", "TIMESTAMP"),
            bigquery.SchemaField("data_particao", "DATE"),
        ]
        table = bigquery.Table(wap_ref, schema=schema)
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="data_particao"
        )
        client.create_table(table)

    storage_client = storage.Client(project=settings.GCP_PROJECT)
    bucket = storage_client.bucket(bucket_name)
    blobs = list(bucket.list_blobs(prefix=source_path))

    partitions_found = {}
    for blob in blobs:
        if blob.name.endswith(".csv") and "data_particao=" in blob.name:
            part = blob.name.split("data_particao=")[1].split("/")[0]
            if part not in partitions_found:
                partitions_found[part] = []
            partitions_found[part].append(blob.name)

    if not partitions_found:
        logger.warning("No partitions found in GCS staging path.")
        return []

    for partition_date, files in partitions_found.items():
        partition_suffix = partition_date.replace("-", "")
        table_ref = f"{settings.GCP_PROJECT}.{dataset_id}.{wap_table_id}${partition_suffix}"

        gcs_uris = [f"gs://{bucket_name}/{f}" for f in files]

        logger.info(f"Loading {len(gcs_uris)} files into WAP partition {partition_date}...")

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            write_disposition="WRITE_TRUNCATE",
            schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
            autodetect=True
        )

        try:
            load_job = client.load_table_from_uri(gcs_uris, table_ref, job_config=job_config)
            load_job.result()
            logger.info(f"Loaded {load_job.output_rows} rows into WAP partition {partition_date}.")
        except Exception as e:
            logger.error(f"BigQuery Load Failed for WAP partition {partition_date}: {e}")
            raise

    return list(partitions_found.keys())


@task(cache_policy=None)
def audit_wap(
    dataset_id: str,
    wap_table_id: str,
    partitions: List[str],
) -> None:
    """
    Audita os dados na tabela WAP antes de promover.
    Verifica: row count > 0 e sem NULLs em colunas críticas.
    Levanta exceção se qualquer check falhar, bloqueando o promote.
    """
    logger = prefect.get_run_logger()
    client = bigquery.Client(project=settings.GCP_PROJECT)
    wap_ref = f"{settings.GCP_PROJECT}.{dataset_id}.{wap_table_id}"

    for partition in partitions:
        logger.info(f"Auditing WAP partition {partition}...")

        count_query = f"""
        SELECT COUNT(*) as row_count
        FROM `{wap_ref}`
        WHERE data_particao = '{partition}'
        """
        result = client.query(count_query).result().to_dataframe()
        row_count = int(result['row_count'].iloc[0])

        if row_count == 0:
            raise ValueError(f"Audit FAILED: partition {partition} has 0 rows in WAP table")
        logger.info(f"Partition {partition}: {row_count} rows - OK")

        null_query = f"""
        SELECT
            COUNTIF(linha_bruta IS NULL OR TRIM(linha_bruta) = '') as null_linha,
            COUNTIF(data_particao IS NULL) as null_particao
        FROM `{wap_ref}`
        WHERE data_particao = '{partition}'
        """
        null_result = client.query(null_query).result().to_dataframe()

        null_linha = int(null_result['null_linha'].iloc[0])
        null_particao = int(null_result['null_particao'].iloc[0])

        if null_linha > 0:
            raise ValueError(
                f"Audit FAILED: partition {partition} has {null_linha} "
                f"rows with NULL/empty linha_bruta"
            )
        if null_particao > 0:
            raise ValueError(
                f"Audit FAILED: partition {partition} has {null_particao} "
                f"rows with NULL data_particao"
            )

        logger.info(f"Partition {partition}: audit PASSED")


@task(cache_policy=None)
def promote_wap(
    dataset_id: str,
    table_id: str,
    wap_table_id: str,
    partitions: List[str],
) -> None:
    """
    Promove dados da tabela WAP para a tabela de staging (produção).
    Para cada partição: DELETE dos dados existentes + INSERT do WAP.
    Idempotente: DELETE + INSERT garante resultado determinístico por partição.
    """
    logger = prefect.get_run_logger()
    client = bigquery.Client(project=settings.GCP_PROJECT)

    staging_ref = f"{settings.GCP_PROJECT}.{dataset_id}.{table_id}"
    wap_ref = f"{settings.GCP_PROJECT}.{dataset_id}.{wap_table_id}"

    try:
        client.get_table(staging_ref)
    except Exception:
        logger.info(f"Staging table {staging_ref} not found. Creating...")
        schema = [
            bigquery.SchemaField("linha_bruta", "STRING"),
            bigquery.SchemaField("timestamp_captura", "TIMESTAMP"),
            bigquery.SchemaField("data_particao", "DATE"),
        ]
        table = bigquery.Table(staging_ref, schema=schema)
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="data_particao"
        )
        client.create_table(table)

    for partition in partitions:
        logger.info(f"Promoting partition {partition} from WAP to staging...")

        delete_query = f"""
        DELETE FROM `{staging_ref}`
        WHERE data_particao = '{partition}'
        """
        client.query(delete_query).result()
        logger.info(f"Deleted existing data for partition {partition} from staging.")

        insert_query = f"""
        INSERT INTO `{staging_ref}`
        SELECT * FROM `{wap_ref}`
        WHERE data_particao = '{partition}'
        """
        client.query(insert_query).result()
        logger.info(f"Inserted data for partition {partition} from WAP to staging.")


@task(cache_policy=None)
def cleanup_staging(
    dataset_id: str,
    wap_table_id: str,
    bucket_name: str,
    staging_path: str,
) -> None:
    """
    Limpa após promoção bem-sucedida: trunca a tabela WAP e
    remove os arquivos do GCS staging. Falhas aqui não bloqueiam o flow.
    """
    logger = prefect.get_run_logger()

    client = bigquery.Client(project=settings.GCP_PROJECT)
    wap_ref = f"{settings.GCP_PROJECT}.{dataset_id}.{wap_table_id}"

    try:
        client.query(f"TRUNCATE TABLE `{wap_ref}`").result()
        logger.info(f"Truncated WAP table {wap_ref}")
    except Exception as e:
        logger.warning(f"Failed to truncate WAP table {wap_ref}: {e}")

    try:
        storage_client = storage.Client(project=settings.GCP_PROJECT)
        bucket = storage_client.bucket(bucket_name)
        blobs = list(bucket.list_blobs(prefix=staging_path))

        for blob in blobs:
            blob.delete()

        logger.info(f"Deleted {len(blobs)} files from gs://{bucket_name}/{staging_path}")
    except Exception as e:
        logger.warning(f"Failed to cleanup GCS staging files: {e}")