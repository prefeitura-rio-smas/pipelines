# pipeline/utils.py
from functools import lru_cache
from typing import Literal
import pandas as pd
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

def fetch_dataframe(
    account: str,
    feature_id: str,
    layer: int,
    where: str = "1=1",
    max_records: int = 5000,
    return_geometry: bool = False,
):
    """Baixa dados sem geometria e devolve DataFrame Polars/Pandas."""
    fl  = get_feature_layer(account, feature_id, layer)
    sdf = fl.query(
        where=where,
        out_fields="*",
        return_geometry=False,
        max_records=max_records,
    ).sdf  # ArcGIS devolve Spatial DataFrame (pandas)
    return sdf

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