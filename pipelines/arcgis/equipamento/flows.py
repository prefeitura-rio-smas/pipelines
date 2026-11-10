from prefect import flow

from pipelines.arcgis.tasks import load_arcgis_to_bigquery


@flow(name="equipamento | Carga ArcGIS")
def equipamento_flow() -> None:
    """
    Fluxo para carregar dados do ArcGIS para o BigQuery para o produto equipamento.

    Só extrai para `arcgis_raw.equipamento_raw`. A transformação não roda mais
    aqui: o antigo modelo legado `unidade` (rj-smas.equipamento.unidade) foi
    substituído por `raw/arcgis/raw_unidade` na medallion, que é materializado
    pelo pipeline dbt (tag weekly).
    """
    # Configuration for this product
    job_name = "equipamento"
    item_id = "1ab2f9c74d8f48239a12157d5a3a87f4"
    return_geometry = False
    layer_idx = 0


    load_arcgis_to_bigquery(
        job_name=job_name,
        item_id=item_id,
        layer_idx=layer_idx,
        return_geometry=return_geometry,
    )

if __name__ == "__main__":
    equipamento_flow()
