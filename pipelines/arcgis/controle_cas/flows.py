from prefect import flow

from pipelines.tasks import run_dbt_models
from pipelines.arcgis.tasks import load_arcgis_to_bigquery


@flow(name="controle_cas | Carga ArcGIS")
def controle_cas() -> None:
    """
    Fluxo para carregar dados do ArcGIS para o BigQuery para o produto controle_cas.
    """
    # Configuration for this product
    job_name = "controle_cas"
    feature_id = "6855307d763b49f6bfb1c5d83b069952"
    return_geometry = False
    batch_size = 20000
    #order_by_field = "objectid"
    layer_name = ""
    layer_idx = 0
    #TODO: Abstrair o tipo de dado do arcgis. Colocar como parametro pra função load_arcgis_to_bigquery ex: type = "layer"

    load_arcgis_to_bigquery(
        job_name=job_name,
        layer_name=layer_name,
        feature_id=feature_id,
        layer_idx=layer_idx,
        return_geometry=return_geometry,
        batch_size=batch_size,
        order_by_field=order_by_field,
        type=type
    )

if __name__ == "__main__":
    controle_cas()


