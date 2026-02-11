from prefect import flow

from pipelines.tasks import run_dbt_models
from pipelines.arcgis.tasks import load_arcgis_to_bigquery


@flow(name="equipamento | Carga ArcGIS")
def equipamento_flow() -> None:
    """
    Fluxo para carregar dados do ArcGIS para o BigQuery para o produto equipamento.
    """
    # Configuration for this product
    job_name = "equipamento"
    item_id = "134851a668ca407c8f6b4cb2fd8dfdf8"
    return_geometry = False
    layer_idx = 0


    load_arcgis_to_bigquery(
        job_name=job_name,
        item_id=item_id,
        layer_idx=layer_idx,
        return_geometry=return_geometry,
    )

    run_dbt_models(model_name="unidade")

if __name__ == "__main__":
    equipamento_flow()
