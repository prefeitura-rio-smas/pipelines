from prefect import flow

from pipelines.tasks import run_dbt_models
from pipelines.arcgis.tasks import load_arcgis_to_bigquery


@flow(name="pipeline_primeira_infancia_carioca_raw_teste | Carga ArcGIS")
def pipeline_primeira_infancia_carioca_raw_teste() -> None:
    """
    Fluxo para carregar dados do ArcGIS para o BigQuery para o produto equipamento.
    """
    # Configuration for this product
    job_name = "pipeline_primeira_infancia_carioca_raw_teste"
    feature_id = "ef6fe5c04520445f91be8a57c4adcd96"
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

    run_dbt_models(model_name="ef6fe5c04520445f91be8a57c4adcd96")

if __name__ == "__main__":
    pipeline_primeira_infancia_carioca_raw_teste_flow()


