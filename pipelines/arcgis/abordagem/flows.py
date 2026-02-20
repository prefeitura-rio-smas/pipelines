import os
from prefect import flow
from prefect_dbt.cli.commands import trigger_dbt_cli_command

from pipelines.arcgis.tasks import load_arcgis_to_bigquery


@flow(name="Abordagem | Carga ArcGIS")
def abordagem_flow() -> None:
    """
    Fluxo para carregar dados do ArcGIS para o BigQuery para o produto abordagem.
    """
    # Configuration for this product
    job_name = "abordagem"
    item_id = "6832ff4ca54c4608b169682ae3a5b088"
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
        command=f"dbt run --select abordagem --target {dbt_target}",
        project_dir="queries",
        profiles_dir="queries",
    )

if __name__ == "__main__":
    abordagem_flow()
