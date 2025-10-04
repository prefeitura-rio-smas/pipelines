# pipeline/tasks.py
from pathlib import Path
import pandas as pd
from google.cloud import bigquery
from .utils import bq_client, dataset_ref, add_timestamp, fetch_dataframe, fetch_features_in_chunks
from datetime import datetime, timezone

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

def extract_arcgis_in_chunks(
    *,
    feature_id: str, 
    account: str = "siurb",
    layer: int = 1,
    where: str = "1=1",
    chunk_size: int = 200000,
    return_geometry: bool = False,
    order_by_field: str = None,
):
    """Extract in chunks (generator)."""
    # Filtra argumentos não nulos para não sobreescrever defaults com None
    kwargs = {
        "account": account,
        "feature_id": feature_id,
        "layer": layer,
        "where": where,
        "return_geometry": return_geometry,
    }
    if chunk_size is not None:
        kwargs["chunk_size"] = chunk_size
    if order_by_field is not None:
        kwargs["order_by_field"] = order_by_field

    return fetch_features_in_chunks(**kwargs)

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

def load_arcgis_to_bigquery(
    *,
    job_name: str,
    layer_name: str,
    feature_id: str,
    layer_idx: int,
    account: str,
    return_geometry: bool,
    chunk_size: int = None,
    order_by_field: str = None,
):
    """
    Extrai dados de uma camada do ArcGIS em chunks e carrega para o BigQuery
    de forma atômica usando uma tabela de staging.
    """
    print(f"↳ Processando {job_name}/{layer_name} (layer {layer_idx})…")
    timestamp = datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')

    # Define as tabelas de destino e de staging
    final_table = f"{job_name}_{layer_name}_raw"
    staging_table = f"{final_table}_staging_{timestamp}"
    
    total_rows = 0
    
    # 1. Extract em Chunks
    chunks = extract_arcgis_in_chunks(
        feature_id=feature_id,
        account=account,
        layer=layer_idx,
        return_geometry=return_geometry,
        chunk_size=chunk_size,
        order_by_field=order_by_field,
    )

    for i, df_chunk in enumerate(chunks):
        if df_chunk.empty:
            continue

        print(f"  • Chunk {i+1}: {len(df_chunk):,} linhas")
        total_rows += len(df_chunk)

        # 2. Stage
        tmp_path = Path(f"/tmp/{staging_table}_{i}.parquet")
        stage_to_parquet(df_chunk, tmp_path)

        # 3. Load para Staging Table
        load_to_bigquery(
            tmp_path,
            staging_table,
            write_disposition="WRITE_APPEND"
        )
        tmp_path.unlink() # Remove o arquivo temporário

    if total_rows == 0:
        print("   • Nada a carregar.")
        return

    # 4. Atomic Replace
    print(f"  • Carga final: {total_rows:,} linhas. Substituindo tabela de produção...")
    client = bq_client()
    query = f"""
    CREATE OR REPLACE TABLE `{dataset_ref()}.{final_table}` AS
    SELECT * FROM `{dataset_ref()}.{staging_table}`
    """
    query_job = client.query(query)
    query_job.result()  # Espera a query terminar

    # 5. Cleanup
    client.delete_table(f"{dataset_ref()}.{staging_table}", not_found_ok=True)
    print(f"   • Tabela `{final_table}` atualizada com sucesso.")
