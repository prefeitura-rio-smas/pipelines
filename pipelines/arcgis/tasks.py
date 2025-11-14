# pipeline/tasks.py
from prefect import task
from prefect import unmapped
from pathlib import Path
import pandas as pd
from google.cloud import bigquery
from datetime import datetime, timezone, timedelta
import prefect
from prefect_dbt.cli.commands import DbtCoreOperation

from .utils import bq_client, dataset_ref, add_timestamp, fetch_dataframe


@task
def extract_arcgis(
    *,
    feature_id: str, 
    account: str = "siurb",
    layer: int = 1,
    where: str = "1=1",
    max_records: int = 5000,
    return_geometry: bool = False,
) -> pd.DataFrame:
    """E = Extract."""
    return fetch_dataframe(
        account=account,
        feature_id=feature_id,
        layer=layer,
        where=where,
        max_records=max_records,
        return_geometry=return_geometry,
    )


@task
def get_feature_layer_metadata(
    feature_id: str,
    layer_idx: int,
    account: str,
):
    """
    Obtém metadados da camada ArcGIS como número total de registros.
    """
    logger = prefect.get_run_logger()
    
    logger.info(f"Obtendo metadados para feature_id: {feature_id}, layer: {layer_idx}, account: {account}")
    
    from .utils import get_feature_layer
    fl = get_feature_layer(account, feature_id, layer_idx)
    
    total_records = fl.query(where="1=1", return_count_only=True)
    
    logger.info(f"Total de registros encontrados: {total_records}")
    
    return total_records


@task
def create_extraction_batches(
    total_records: int,
    batch_size: int = 100000,  # Tamanho reduzido para evitar sobrecarga no servidor ArcGIS
):
    """
    Cria uma lista de batches para processamento em paralelo.
    """
    logger = prefect.get_run_logger()
    
    logger.info(f"Criando batches para {total_records} registros com batch_size {batch_size}")
    
    batches = []
    for offset in range(0, total_records, batch_size):
        current_batch_size = min(batch_size, total_records - offset)
        batches.append({
            "offset": offset,
            "batch_size": current_batch_size
        })
    
    logger.info(f"Total de batches criados: {len(batches)}")
    
    return batches


@task(retries=3, retry_delay_seconds=30)
def extract_batch_data(
    feature_id: str,
    layer_idx: int,
    account: str,
    offset: int,
    batch_size: int,
    return_geometry: bool = False,
    order_by_field: str = None,
    where: str = "1=1",
):
    """
    Extrai um único batch de dados do ArcGIS.
    """
    logger = prefect.get_run_logger()
    
    logger.info(f"Extraindo batch: offset={offset}, batch_size={batch_size}")
    
    from .utils import get_feature_layer
    fl = get_feature_layer(account, feature_id, layer_idx)
    
    query_params = {
        "where": where,
        "out_fields": "*",
        "return_geometry": return_geometry,
        "result_offset": offset,
        "result_record_count": batch_size,
    }
    if order_by_field:
        query_params["order_by_fields"] = order_by_field

    sdf = fl.query(**query_params).sdf
    
    logger.info(f"Batch extraído com sucesso: {len(sdf)} registros")
    
    return sdf


@task
def save_batch_to_staging(
    batch_data: pd.DataFrame,
    staging_table: str,
    batch_index: int,
):
    """
    Salva localmente e carrega um batch para a tabela staging no BigQuery.
    """
    logger = prefect.get_run_logger()
    
    if batch_data.empty:
        logger.info(f"Batch {batch_index} está vazio, pulando...")
        return 0
    
    logger.info(f"Processando batch {batch_index}: {len(batch_data)} registros")
    
    # 1. Adiciona timestamp
    from .utils import add_timestamp
    add_timestamp(batch_data)
    
    # 2. Salva localmente em parquet
    tmp_path = Path(f"/tmp/{staging_table}_{batch_index}.parquet")
    tmp_path.parent.mkdir(parents=True, exist_ok=True)
    batch_data.to_parquet(tmp_path, index=False)
    
    logger.info(f"Batch {batch_index} salvo localmente: {tmp_path}")
    
    # 3. Carrega para BigQuery staging
    from .utils import bq_client, dataset_ref
    client = bq_client()
    job_cfg = bigquery.LoadJobConfig(
        autodetect=True,
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition="WRITE_APPEND",  # Permite múltiplas tasks escreverem na mesma tabela
    )
    
    with tmp_path.open("rb") as f:
        job = client.load_table_from_file(
            f,
            destination=f"{dataset_ref()}.{staging_table}",
            job_config=job_cfg,
        )
    rows_loaded = job.result().output_rows  # Espera terminar e obtém resultado
    
    logger.info(f"Batch {batch_index} carregado para staging: {rows_loaded} linhas")
    
    # 4. Remove o arquivo temporário
    tmp_path.unlink()
    
    return rows_loaded


@task
def atomic_replace_raw_table(
    final_table: str,
    staging_table: str,
):
    """
    Faz substituição atômica da tabela final com dados da staging.
    """
    logger = prefect.get_run_logger()
    
    logger.info(f"Substituindo tabela final '{final_table}' com dados da staging '{staging_table}'")
    
    from .utils import bq_client, dataset_ref
    client = bq_client()
    query = f"""
    CREATE OR REPLACE TABLE `{dataset_ref()}.{final_table}` AS
    SELECT * FROM `{dataset_ref()}.{staging_table}`
    """
    
    query_job = client.query(query)
    query_job.result()  # Espera a query terminar
    
    logger.info(f"Tabela '{final_table}' atualizada com sucesso")
    
    # Remove tabela staging
    client.delete_table(f"{dataset_ref()}.{staging_table}", not_found_ok=True)
    logger.info(f"Tabela staging '{staging_table}' removida")


@task
def load_arcgis_to_bigquery(
    *,
    job_name: str,
    layer_name: str,
    feature_id: str,
    layer_idx: int,
    account: str,
    return_geometry: bool,
    batch_size: int = None,
    order_by_field: str = None,
):
    """
    Extrai dados de uma camada do ArcGIS em batches e carrega para o BigQuery
    de forma atômica usando uma tabela de staging.
    """
    logger = prefect.get_run_logger()
    
    logger.info(f"↳ Processando {job_name}/{layer_name} (layer {layer_idx})…")
    logger.info(f"Parâmetros - job_name: {job_name}, layer_name: {layer_name}, feature_id: {feature_id}, layer: {layer_idx}, account: {account}")
    
    timestamp = datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')

    # Define as tabelas de destino e de staging
    final_table = f"{job_name}_{layer_name}_raw"
    staging_table = f"{final_table}_staging_{timestamp}"
    
    logger.info(f"Nome da tabela final: {final_table}")
    logger.info(f"Nome da tabela staging: {staging_table}")

    # 1. Obter metadados
    total_records = get_feature_layer_metadata(
        feature_id=feature_id,
        layer_idx=layer_idx,
        account=account
    )
    
    if total_records == 0:
        logger.info("Nenhum registro encontrado")
        # Mesmo que não tenha dados, criamos uma tabela vazia
        from .utils import bq_client, dataset_ref
        client = bq_client()
        query = f"""
        CREATE OR REPLACE TABLE `{dataset_ref()}.{final_table}` AS
        SELECT * FROM `{dataset_ref()}.{staging_table}`
        WHERE 1=0
        """
        query_job = client.query(query)
        query_job.result()
        client.delete_table(f"{dataset_ref()}.{staging_table}", not_found_ok=True)
        logger.info(f"Tabela `{final_table}` atualizada (tabela vazia)")
        return

    # 2. Criar batches
    batches_info = create_extraction_batches(
        total_records=total_records,
        batch_size=batch_size or 100000  # Tamanho de batch reduzido para evitar sobrecarga
    )
    
    logger.info(f"Iniciando processamento de {len(batches_info)} batches...")

    # 3. Processar batches com controle de concorrência (usando map)
    if batches_info:
        # Extração em paralelo com controle de concorrência
        batch_data_list = extract_batch_data.map(
            feature_id=unmapped(feature_id),
            layer_idx=unmapped(layer_idx),
            account=unmapped(account),
            offset=[batch["offset"] for batch in batches_info],
            batch_size=[batch["batch_size"] for batch in batches_info],
            return_geometry=unmapped(return_geometry),
            order_by_field=unmapped(order_by_field),
        )
        
        # Carregar batches para staging em paralelo
        loaded_batches = save_batch_to_staging.map(
            batch_data=batch_data_list,
            staging_table=unmapped(staging_table),
            batch_index=[i for i in range(len(batches_info))]
        )
        
        # Aguardar todos os batches serem processados e somar total
        # No Prefect 3, precisamos esperar os resultados das tasks mapeadas
        total_rows = 0
        for loaded_batch in loaded_batches:
            if loaded_batch is not None:
                total_rows += loaded_batch.result()  # Espera o resultado de cada task
    else:
        total_rows = 0
        logger.info("Nenhum batch a ser processado")

    # 4. Substituição atômica
    if total_rows > 0:
        atomic_replace_raw_table(
            final_table=final_table,
            staging_table=staging_table
        )
        logger.info(f"   • Tabela `{final_table}` atualizada com sucesso ({total_rows:,} linhas).")
    else:
        logger.info("   • Nada a carregar - total_rows = 0")
        from .utils import bq_client, dataset_ref
        client = bq_client()
        query = f"""
        CREATE OR REPLACE TABLE `{dataset_ref()}.{final_table}` AS
        SELECT * FROM `{dataset_ref()}.{staging_table}`
        WHERE 1=0
        """
        query_job = client.query(query)
        query_job.result()
        client.delete_table(f"{dataset_ref()}.{staging_table}", not_found_ok=True)
        logger.info(f"Tabela `{final_table}` atualizada (tabela vazia)")

# Diretório do projeto dbt (pasta paralela `queries`)
DBT_PROJECT_DIR = Path(__file__).parent.parent / "../queries"

@task
def run_dbt_models(model_name: str = None):
    """
    Executa os modelos do dbt usando a integração prefect-dbt.
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
        profiles_dir=DBT_PROJECT_DIR, # Assumindo que profiles.yml está no mesmo diretório
    )

    result = dbt_run_op.run()
    
    logger.info(f"✅ dbt model {model_name} concluído com sucesso.")
    return result
