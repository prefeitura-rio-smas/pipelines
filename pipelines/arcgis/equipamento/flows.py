import os
from prefect import flow
from prefect_dbt.cli.commands import trigger_dbt_cli_command

from pipelines.arcgis.tasks import load_arcgis_to_bigquery


@flow(name="equipamento | Carga ArcGIS")
def equipamento_flow() -> None:
    """
    Fluxo para carregar dados do ArcGIS para o BigQuery para o produto equipamento.
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

    dbt_target = os.getenv("MODE", "staging")
    trigger_dbt_cli_command(
        command=f"dbt run --select unidade --target {dbt_target}",
        project_dir="queries",
        profiles_dir="queries",
    )

if __name__ == "__main__":
    equipamento_flow()
