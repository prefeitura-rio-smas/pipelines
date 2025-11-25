# pipeline/tasks.py
from prefect import task
from prefect import unmapped
from pathlib import Path
import pandas as pd
from google.cloud import bigquery
from datetime import datetime, timezone, timedelta
import prefect
from prefect_dbt.cli.commands import DbtCoreOperation

from .utils import bq_client, dataset_ref, add_timestamp


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

    from .utils import get_layer_service_url, _get_arcgis_token
    import requests
    
    service_url = get_layer_service_url(account, feature_id)
    token = _get_arcgis_token(account)
    url = f"{service_url}/{layer_idx}/query"
    
    params = {
        "where": "1=1",
        "returnCountOnly": "true",
        "f": "json",
        "token": token,
    }
    
    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        total_records = data.get("count")
        if total_records is None:
            total_records = 0
            
    except requests.exceptions.RequestException as e:
        logger.error(f"Erro ao conectar com o servidor: {e}")
        raise ValueError(f"Erro ao conectar com o servidor: {e}")

    logger.info(f"Total de registros encontrados: {total_records}")
    return total_records


@task
def save_batch_to_staging(
    batch_data: pd.DataFrame,
    staging_table: str,
    batch_index: int,
    bq_schema: list,
):
    """
    Salva localmente e carrega um batch para a tabela staging no BigQuery, forçando colunas de origem para STRING.
    """
    logger = prefect.get_run_logger()

    if batch_data.empty and batch_index > 0:
        logger.info(f"Batch {batch_index} está vazio, pulando...")
        return 0

    logger.info(f"Processando batch {batch_index}: {len(batch_data)} registros")

    from .utils import add_timestamp
    if not batch_data.empty:
        add_timestamp(batch_data)

    # Força todas as colunas de origem a serem STRING, exceto as que criamos.
    # Isso está alinhado com a estratégia ELT para evitar erros de tipo de dados na carga.
    source_columns = [field.name for field in bq_schema if field.field_type == 'STRING']
    for col_name in source_columns:
        if col_name in batch_data.columns:
            batch_data[col_name] = batch_data[col_name].astype(str)

    tmp_path = Path(f"/tmp/{staging_table}_{batch_index}.parquet")
    tmp_path.parent.mkdir(parents=True, exist_ok=True)
    batch_data.to_parquet(tmp_path, index=False)

    logger.info(f"Batch {batch_index} salvo localmente: {tmp_path}")

    from .utils import bq_client, dataset_ref
    client = bq_client()

    write_disposition = "WRITE_APPEND"
    if batch_index == 0:
        write_disposition = "WRITE_TRUNCATE"

    job_cfg = bigquery.LoadJobConfig(
        schema=bq_schema,
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=write_disposition,
    )

    with tmp_path.open("rb") as f:
        job = client.load_table_from_file(
            f,
            destination=f"{dataset_ref()}.{staging_table}",
            job_config=job_cfg,
        )
    rows_loaded = job.result().output_rows

    logger.info(f"Batch {batch_index} carregado para staging: {rows_loaded} linhas")
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
    query = f"CREATE OR REPLACE TABLE `{dataset_ref()}.{final_table}` AS SELECT * FROM `{dataset_ref()}.{staging_table}`"
    query_job = client.query(query)
    query_job.result()
    logger.info(f"Tabela '{final_table}' atualizada com sucesso")
    client.delete_table(f"{dataset_ref()}.{staging_table}", not_found_ok=True)
    logger.info(f"Tabela staging '{staging_table}' removida")


@task
def get_layer_info(feature_id: str, layer_idx: int, account: str) -> dict:
    """Gets the schema and CRS of an ArcGIS layer."""
    logger = prefect.get_run_logger()
    logger.info(f"Obtendo schema e CRS para feature_id: {feature_id}, layer: {layer_idx}")
    from .utils import get_layer_service_url, _get_arcgis_token
    import requests
    service_url = get_layer_service_url(account, feature_id)
    token = _get_arcgis_token(account)
    url = f"{service_url}/{layer_idx}"
    params = {"f": "json", "token": token}
    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        return {
            "fields": data.get("fields", []),
            "crs": data.get("spatialReference", {})
        }
    except requests.exceptions.RequestException as e:
        logger.error(f"Erro ao obter schema e CRS: {e}")
        raise ValueError(f"Erro ao obter schema e CRS: {e}")

def arcgis_to_bq_schema(arcgis_fields: list, return_geometry: bool) -> list:
    """Converts an ArcGIS fields list to a BigQuery schema list, treating ALL source fields as STRING."""
    bq_schema = []
    if not arcgis_fields:
        return bq_schema
    for field in arcgis_fields:
        bq_schema.append(bigquery.SchemaField(field['name'], "STRING"))
    
    # Also add columns that might be added during processing
    bq_schema.append(bigquery.SchemaField("timestamp_captura", "TIMESTAMP"))
    if return_geometry:
        bq_schema.append(bigquery.SchemaField("longitude", "FLOAT64"))
        bq_schema.append(bigquery.SchemaField("latitude", "FLOAT64"))
        bq_schema.append(bigquery.SchemaField("geometry", "GEOGRAPHY"))
    return bq_schema


@task
def load_arcgis_to_bigquery(
    *,
    job_name: str,
    layer_name: str,
    feature_id: str,
    layer_idx: int,
    account: str,
    return_geometry: bool,
    batch_size: int = 20000,
    order_by_field: str = None,
    where_clause: str = "1=1"
):
    """
    Extracts data from an ArcGIS layer and loads it into BigQuery.
    """
    logger = prefect.get_run_logger()
    logger.info(f"↳ Processando {job_name}/{layer_name} (layer {layer_idx})…")

    timestamp = datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')
    final_table = f"{job_name}_{layer_name}_raw"
    staging_table = f"{final_table}_staging_{timestamp}"
    logger.info(f"Tabela final: {final_table}, Tabela de Staging: {staging_table}")

    layer_info = get_layer_info(feature_id=feature_id, layer_idx=layer_idx, account=account)
    total_records = get_feature_layer_metadata(feature_id=feature_id, layer_idx=layer_idx, account=account)
    bq_schema = arcgis_to_bq_schema(layer_info["fields"], return_geometry)
    source_crs_wkid = layer_info.get("crs", {}).get("wkid")

    if total_records == 0:
        logger.info("Nenhum registro encontrado. Criando ou limpando a tabela final.")
        from .utils import bq_client, dataset_ref
        client = bq_client()
        table_id = f"{dataset_ref()}.{final_table}"
        client.delete_table(table_id, not_found_ok=True)
        table = bigquery.Table(table_id, schema=bq_schema)
        client.create_table(table)
        logger.info(f"Tabela `{final_table}` criada vazia com o schema correto.")
        return

    logger.info("Iniciando extração sequencial por batches...")
    
    offset = 0
    total_rows_loaded = 0
    batch_index = 0

    while offset < total_records:
        logger.info(f"Extraindo batch {batch_index}: offset={offset}, batch_size={batch_size}")

        import requests
        from .utils import get_layer_service_url, _get_arcgis_token
        
        service_url = get_layer_service_url(account, feature_id)
        token = _get_arcgis_token(account)
        url = f"{service_url}/{layer_idx}/query"
        
        params = {
            "where": where_clause, "outFields": "*", "returnGeometry": str(return_geometry).lower(),
            "f": "json", "resultOffset": offset, "resultRecordCount": batch_size,
            "token": token,
        }
        if order_by_field:
            params["orderByFields"] = order_by_field

        try:
            response = requests.post(url, data=params, timeout=120)
            response.raise_for_status()
            data = response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Erro ao conectar com o servidor no batch {batch_index}: {e}")
            raise ValueError(f"Erro no batch {batch_index}: {e}")

        features = data.get("features", [])
        num_features_received = len(features)
        logger.info(f"Batch {batch_index} extraído: {num_features_received} registros recebidos.")

        if num_features_received == 0 and offset < total_records:
             # If we expected records but got none, something is wrong, but we can try to continue
             logger.warning(f"Recebido 0 registros no offset {offset} mas total de registros é {total_records}. Interrompendo.")
             break
        elif num_features_received == 0:
            logger.info("Recebido 0 registros. Fim da extração.")
            break

        import geopandas as gpd
        from shapely.geometry import Point, Polygon, LineString

        processed_data = []
        for feature in features:
            attributes = feature.get("attributes", {})
            if return_geometry:
                geometry_data = feature.get("geometry", {})
                geom = None
                if "rings" in geometry_data: geom = Polygon(geometry_data["rings"][0], geometry_data["rings"][1:])
                elif "x" in geometry_data and "y" in geometry_data: geom = Point(geometry_data.get("x"), geometry_data.get("y"))
                elif "paths" in geometry_data: geom = LineString(geometry_data["paths"][0])
                if geom: attributes["geometry"] = geom
            processed_data.append(attributes)

        batch_df = pd.DataFrame(processed_data)

        if return_geometry and "geometry" in batch_df.columns:
            gdf = gpd.GeoDataFrame(batch_df, geometry='geometry')
            gdf.geometry = gdf.geometry.buffer(0)
            if not gdf.empty:
                first_geom = gdf.geometry.dropna().iloc[0] if not gdf.geometry.dropna().empty else None
                if first_geom:
                    rep_point = first_geom.representative_point()
                    x, y = rep_point.x, rep_point.y
                    if (abs(y) > 90 or abs(x) > 180) and source_crs_wkid:
                        logger.info(f"Reprojetando de EPSG:{source_crs_wkid} para EPSG:4326...")
                        gdf.set_crs(epsg=source_crs_wkid, inplace=True, allow_override=True)
                        gdf = gdf.to_crs("EPSG:4326")
                        logger.info("Reprojeção concluída.")
                    rep_points = gdf.geometry.representative_point()
                    gdf["longitude"] = rep_points.x
                    gdf["latitude"] = rep_points.y
            batch_df = gdf

        rows_loaded = save_batch_to_staging.fn(batch_data=batch_df, staging_table=staging_table, batch_index=batch_index, bq_schema=bq_schema)
        total_rows_loaded += rows_loaded
        offset += num_features_received
        batch_index += 1
        
        if not data.get("exceededTransferLimit", False):
            break

    if total_rows_loaded > 0:
        atomic_replace_raw_table(final_table=final_table, staging_table=staging_table)
        logger.info(f"   • Tabela `{final_table}` atualizada com sucesso ({total_rows_loaded:,} linhas).")
    else:
        logger.info("   • Nada a carregar - total_rows = 0. Limpando tabela final se existir.")
        from .utils import bq_client, dataset_ref
        client = bq_client()
        table_id = f"{dataset_ref()}.{final_table}"
        client.delete_table(table_id, not_found_ok=True)
        table = bigquery.Table(table_id, schema=bq_schema)
        client.create_table(table)
        client.delete_table(f"{dataset_ref()}.{staging_table}", not_found_ok=True)


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
        profiles_dir=DBT_PROJECT_DIR,
    )

    result = dbt_run_op.run()

    logger.info(f"✅ dbt model {model_name} concluído com sucesso.")
    return result