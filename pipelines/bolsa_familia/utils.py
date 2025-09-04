# pipeline/utils.py
from functools import lru_cache
from typing import Literal
import pandas as pd
from google.cloud     import bigquery, storage
from .constants       import settings
from datetime         import datetime, timezone


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