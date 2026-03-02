import os
from prefect import flow
from prefect_dbt.cli.commands import trigger_dbt_cli_command
from pipelines.acolherio.constants import settings

import os
from pathlib import Path
from prefect import get_run_logger

logger = get_run_logger()

logger.info(f"CWD: {os.getcwd()}")
logger.info(f"FILES: {[p.name for p in Path('.').iterdir()]}")


@flow(name="dbt_acolherio_rma")
def acolherio_flow() -> None:
    
    dbt_target = os.getenv("MODE", "staging")
    trigger_dbt_cli_command(
        command=f"dbt run --select stg_filtro_evolucao --target {dbt_target}",
        profiles_dir="."
    )
    
if __name__ == "__main__":
    acolherio_flow()
