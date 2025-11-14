# pipeline/utils.py
from functools import lru_cache
from typing import Literal
import pandas as pd
import geopandas as gpd
from arcgis.gis       import GIS
from arcgis.features  import FeatureLayer
from google.cloud     import bigquery, storage
from .constants       import settings
from datetime         import datetime, timezone

# ---------- ArcGIS ----------
def _get_gis(account: Literal["siurb", "agol"]) -> GIS:
    import prefect
    logger = prefect.get_run_logger()
    
    if account == "siurb":
        logger.info(f"Conectando ao GIS SIURB: {settings.SIURB_URL} com usuário: {settings.SIURB_USER}")
        gis = GIS(settings.SIURB_URL, settings.SIURB_USER, settings.SIURB_PWD)
    elif account == "agol":
        logger.info(f"Conectando ao GIS AGOL: {settings.AGOL_URL} com usuário: {settings.AGOL_USER}")
        gis = GIS(settings.AGOL_URL, settings.AGOL_USER, settings.AGOL_PWD)
    else:
        raise ValueError("account deve ser 'siurb' ou 'agol'")
    
    logger.info(f"Autenticação bem-sucedida para conta {account}")
    return gis

def get_feature_layer(account: str, feature_id: str, layer: int) -> FeatureLayer:
    import prefect
    logger = prefect.get_run_logger()
    
    logger.info(f"Buscando feature layer - conta: {account}, feature_id: {feature_id}, layer: {layer}")
    
    gis = _get_gis(account)
    item = gis.content.get(feature_id)
    
    if item is None:
        logger.error(f"Feature item não encontrado: {feature_id} para a conta {account}")
        raise ValueError(f"Feature item não encontrado: {feature_id}")
    
    logger.info(f"Item encontrado: {item.title} ({item.id})")
    
    if len(item.layers) <= layer:
        logger.error(f"Layer {layer} não existe. Total de layers: {len(item.layers)}")
        raise ValueError(f"Layer {layer} não existe no item {feature_id}. Total de layers: {len(item.layers)}")
    
    feature_layer = item.layers[layer]
    logger.info(f"Feature layer encontrado: {feature_layer.properties.name if hasattr(feature_layer.properties, 'name') else 'N/A'}")
    
    return feature_layer 

def fetch_dataframe(
    account: str,
    feature_id: str,
    layer: int,
    where: str = "1=1",
    max_records: int = 5000,
    return_geometry: bool = False,
):
    """Baixa dados e devolve DataFrame Polars/Pandas."""
    import prefect
    logger = prefect.get_run_logger()
    
    logger.info(f"Iniciando fetch_dataframe - conta: {account}, feature_id: {feature_id}, layer: {layer}")
    fl  = get_feature_layer(account, feature_id, layer)
    logger.info(f"Executando query com where: {where}, max_records: {max_records}")
    
    try:
        sdf = fl.query(
            where=where,
            out_fields="*",
            return_geometry=return_geometry,
            max_records=max_records,
        ).sdf  # ArcGIS devolve Spatial DataFrame (pandas)
        logger.info(f"Query concluída com sucesso, recebido {len(sdf)} registros")
    except Exception as e:
        logger.error(f"Erro ao executar query: {str(e)}")
        raise

    if return_geometry and not sdf.empty and "SHAPE" in sdf.columns:
        logger.info("Processando geometrias...")
        gdf = gpd.GeoDataFrame(sdf, geometry='SHAPE')

        # Fix invalid geometries
        gdf.geometry = gdf.geometry.buffer(0)

        if sdf.spatial.sr:
            gdf = gdf.set_crs(epsg=sdf.spatial.sr['wkid'])
            gdf = gdf.to_crs("EPSG:4326")

        # Check if the geometry type is Point and add lat/lon
        if not gdf.empty and gdf.geom_type.iloc[0] == 'Point':
            gdf['longitude'] = gdf.geometry.x
            gdf['latitude'] = gdf.geometry.y
        
        logger.info("Geometrias processadas com sucesso")
        return gdf

    return sdf


def fetch_features_in_chunks(
    account: str,
    feature_id: str,
    layer: int,
    where: str = "1=1",
    chunk_size: int = 200000, # Default alto, será sobreposto pelo YAML
    return_geometry: bool = False,
    order_by_field: str = None,
):
    """
    Busca features em lotes (chunks) e retorna um gerador de DataFrames.
    """
    import prefect
    logger = prefect.get_run_logger()
    
    logger.info(f"Iniciando fetch_features_in_chunks - conta: {account}, feature_id: {feature_id}, layer: {layer}")
    
    fl = get_feature_layer(account, feature_id, layer)

    # 1. Obter o número total de registros
    logger.info(f"Obtendo contagem total de registros com where: {where}")
    try:
        total_records = fl.query(where=where, return_count_only=True)
        logger.info(f"Total de registros encontrados: {total_records}")
    except Exception as e:
        logger.error(f"Erro ao obter contagem total de registros: {str(e)}")
        raise

    # 2. Iterar em chunks
    logger.info(f"Iniciando iteração em chunks, total_records: {total_records}, chunk_size: {chunk_size}")
    for i, offset in enumerate(range(0, total_records, chunk_size)):
        records_to_fetch = min(chunk_size, total_records - offset)
        logger.info(f"Processando chunk {i+1}: offset={offset}, records_to_fetch={records_to_fetch}")

        query_params = {
            "where": where,
            "out_fields": "*",
            "return_geometry": return_geometry,
            "result_offset": offset,
            "result_record_count": records_to_fetch,
        }
        if order_by_field:
            query_params["order_by_fields"] = order_by_field

        try:
            sdf = fl.query(**query_params).sdf
            logger.info(f"Chunk {i+1}: recebido {len(sdf)} registros")
        except Exception as e:
            logger.error(f"Erro ao consultar chunk {i+1} (offset={offset}): {str(e)}")
            raise

        if sdf.empty:
            logger.info(f"Chunk {i+1} está vazio, pulando...")
            continue

        if return_geometry and "SHAPE" in sdf.columns:
            logger.info(f"Processando geometrias para chunk {i+1}")
            gdf = gpd.GeoDataFrame(sdf, geometry='SHAPE')
            gdf.geometry = gdf.geometry.buffer(0)
            if sdf.spatial.sr:
                gdf = gdf.set_crs(epsg=sdf.spatial.sr['wkid']).to_crs("EPSG:4326")
            if not gdf.empty and gdf.geom_type.iloc[0] == 'Point':
                gdf['longitude'] = gdf.geometry.x
                gdf['latitude'] = gdf.geometry.y
            logger.info(f"Yielding geodataframe com {len(gdf)} registros")
            yield gdf
        else:
            logger.info(f"Yielding dataframe com {len(sdf)} registros")
            yield sdf

# ---------- BigQuery / GCS ----------
@lru_cache
def bq_client() -> bigquery.Client:
    return bigquery.Client(project=settings.GCP_PROJECT)

@lru_cache
def gcs_client() -> storage.Client:
    return storage.Client(project=settings.GCP_PROJECT)

def dataset_ref() -> str:
    return f"{settings.GCP_PROJECT}.{settings.GCP_DATASET}"

# ---------- Outros ----------
def add_timestamp(df: pd.DataFrame, column="timestamp_captura") -> pd.DataFrame:
    """
    Acrescenta coluna ISO-8601 UTC (AAAA-MM-DDTHH:MM:SS) ao DataFrame.
    Retorna a mesma instância (conveniente para encadear).
    """
    df[column] = datetime.now(tz=timezone.utc).isoformat(timespec="seconds")
    return df