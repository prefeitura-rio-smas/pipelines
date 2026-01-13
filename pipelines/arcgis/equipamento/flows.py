from prefect import flow

from pipelines.tasks import run_dbt_models
from pipelines.arcgis.tasks import load_arcgis_to_bigquery


flow(name="equipamento" | Carga ArcGIS")
def equipamento_flow() -> None:
    """
    Fluxo para carregar dados do ArcGIS para o BigQuery para o produto equipamento.
    """
    # Configuration for this product
    job_name = "equipamento"
    feature_id = "134851a668ca407c8f6b4cb2fd8dfdf8"
    return_geometry = False
    batch_size = 20000
    order_by_field = "objectid"
    layer_name = "repeat"
    layer_idx = 1


    load_arcgis_to_bigquery(
        job_name=job_name,
        layer_name=layer_name,
        feature_id=feature_id,
        layer_idx=layer_idx,
        return_geometry=return_geometry,
        batch_size=batch_size,
        order_by_field=order_by_field,
    )

    run_dbt_models(model_name="equipamento")

if __name__ == "__main__":
    abordagem_flow()


