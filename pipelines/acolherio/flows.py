import os
from prefect import flow
from prefect_dbt.cli.commands import trigger_dbt_cli_command
from pipelines.acolherio.constants import settings


@flow(name="dbt_acolherio_rma")
def acolherio_flow() -> None:
    
    dbt_target = os.getenv("MODE", "staging")
    trigger_dbt_cli_command(
        command=f"dbt run --select stg_filtro_evolucao --target {dbt_target}",
        project_dir="queries",
        profiles_dir="queries",
    )
    
if __name__ == "__main__":
    acolherio_flow()
