# pipeline/tasks.py
from pathlib import Path
import pandas as pd
from google.cloud import bigquery
from .utils import bq_client, dataset_ref, add_timestamp


def stage_to_parquet(df: pd.DataFrame, path: Path) -> Path:
    """Salva dataframe localmente em parquet (formato rápido)."""
    add_timestamp(df)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)
    return path

def load_to_bigquery(
    path: Path,
    table: str,
    *,
    write_disposition: bigquery.WriteDisposition = "WRITE_TRUNCATE",
):
    """L = Load (carrega parquet -> BQ via load_job)."""
    client = bq_client()
    job_cfg = bigquery.LoadJobConfig(
        autodetect=True,
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=write_disposition,
    )
    with path.open("rb") as f:
        job = client.load_table_from_file(
            f,
            destination=f"{dataset_ref()}.{table}",
            job_config=job_cfg,
        )
    job.result()  # espera terminar

    return job.output_rows