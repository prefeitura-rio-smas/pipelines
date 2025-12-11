from prefect import flow

from pipelines.arcgis.tasks import load_arcgis_to_bigquery, run_dbt_models


@flow(name="Abordagem | Carga ArcGIS")
def abordagem_flow() -> None:
    """
    Fluxo para carregar dados do ArcGIS para o BigQuery para o produto abordagem.
    """
    # Configuration for this product
    job_name = "abordagem"
    feature_id = "6832ff4ca54c4608b169682ae3a5b088"
    account = "siurb"
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
        account=account,
        return_geometry=return_geometry,
        batch_size=batch_size,
        order_by_field=order_by_field,
    )

    run_dbt_models(model_name="abordagem")

if __name__ == "__main__":
    abordagem_flow()
