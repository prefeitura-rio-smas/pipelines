import os
from prefect import flow
from prefect_dbt.cli.commands import trigger_dbt_cli_command

from pipelines.arcgis.tasks import load_arcgis_to_bigquery


@flow(name="cras_cas_poligonos | Carga ArcGIS")
def cras_cas_poligonos_flow() -> None:
    """
    Fluxo para carregar dados do ArcGIS para o BigQuery para o produto cras_cas_poligonos.
    """
    # Configuration for this product
    job_name = "cras_cas_poligonos"
    item_id = "e56a0700682b4dba96aca950fb3d96a3"
    return_geometry = False
    batch_size = 20000
    order_by_field = "objectid"
    layer_name = "repeat"
    layer_idx = 1


    load_arcgis_to_bigquery(
        job_name=job_name,
        layer_name=layer_name,
        item_id=item_id,
        layer_idx=layer_idx,
        return_geometry=return_geometry,
        batch_size=batch_size,
        order_by_field=order_by_field,
    )

    dbt_target = os.getenv("MODE", "staging")
    trigger_dbt_cli_command(
        command=f"dbt run --select cras_cas_poligonos --target {dbt_target}",
        project_dir="queries",
        profiles_dir="queries",
    )

if __name__ == "__main__":
    cras_cas_poligonos_flow()


