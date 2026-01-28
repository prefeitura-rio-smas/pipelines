from prefect import flow

from pipelines.tasks import run_dbt_models
from pipelines.arcgis.tasks import load_arcgis_to_bigquery


@flow(name="controle_cas | Carga ArcGIS")
def controle_cas() -> None:
    """
    Fluxo para carregar dados do ArcGIS para o BigQuery para o produto cras_cas_poligonos.
    """
    # Configuration for this product
    job_name = "controle_cas"
    feature_id = "6855307d763b49f6bfb1c5d83b069952"
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

    run_dbt_models(model_name="controle_cas")

if __name__ == "__main__":
    controle-cas()


