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
    if account == "siurb":
        return GIS(settings.SIURB_URL, settings.SIURB_USER, settings.SIURB_PWD)
    elif account == "agol":
        return GIS(settings.AGOL_URL, settings.AGOL_USER, settings.AGOL_PWD)
    else:
        raise ValueError("account deve ser 'siurb' ou 'agol'")

def get_feature_layer(account: str, feature_id: str, layer: int) -> FeatureLayer:
    gis  = _get_gis(account)
    item = gis.content.get(
        feature_id
    )
    return item.layers[layer] 

def get_table(account: str, feature_id: str, layer: int) -> FeatureLayer:
    gis  = _get_gis(account)
    item = gis.content.get(
        feature_id
    )
    return item.tables[layer]

def get_source(account: str, feature_id: str, layer: int, source_type: str) -> FeatureLayer:
    if source_type == "table":
        return get_table(account, feature_id, layer)
    # Default to layer
    return get_feature_layer(account, feature_id, layer)


def fetch_dataframe(
    account: str,
    feature_id: str,
    layer: int,
    where: str = "1=1",
    max_records: int = 5000,
    return_geometry: bool = False,
    source_type: str = "layer",
):
    """Baixa dados e devolve DataFrame Polars/Pandas."""
    fl  = get_source(account, feature_id, layer, source_type)
    sdf = fl.query(
        where=where,
        out_fields="*",
        return_geometry=return_geometry,
        max_records=max_records,
    ).sdf  # ArcGIS devolve Spatial DataFrame (pandas)

    if return_geometry and not sdf.empty and "SHAPE" in sdf.columns:
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
    source_type: str = "layer",
):
    """
    Busca features em lotes (chunks) e retorna um gerador de DataFrames.
    """
    fl = get_source(account, feature_id, layer, source_type)
    
    # 1. Obter o número total de registros
    total_records = fl.query(where=where, return_count_only=True)
    
    # 2. Iterar em chunks
    for offset in range(0, total_records, chunk_size):
        records_to_fetch = min(chunk_size, total_records - offset)

        query_params = {
            "where": where,
            "out_fields": "*",
            "return_geometry": return_geometry,
            "result_offset": offset,
            "result_record_count": records_to_fetch,
        }
        if order_by_field:
            query_params["order_by_fields"] = order_by_field

        sdf = fl.query(**query_params).sdf
        
        if sdf.empty:
            continue

        if return_geometry and "SHAPE" in sdf.columns:
            gdf = gpd.GeoDataFrame(sdf, geometry='SHAPE')
            gdf.geometry = gdf.geometry.buffer(0)
            if sdf.spatial.sr:
                gdf = gdf.set_crs(epsg=sdf.spatial.sr['wkid']).to_crs("EPSG:4326")
            if not gdf.empty and gdf.geom_type.iloc[0] == 'Point':
                gdf['longitude'] = gdf.geometry.x
                gdf['latitude'] = gdf.geometry.y
            yield gdf
        else:
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