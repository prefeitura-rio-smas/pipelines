"""Transformação — dbt genérico (tag-driven)."""
import os
from prefect import flow
from prefect_dbt.cli.commands import trigger_dbt_cli_command


@flow(name="Transformação | dbt")
def dbt_transform_flow(select: str | None = None):
    dbt_target = os.getenv("MODE", "staging")
    cmd = "dbt build"
    if select:
        cmd += f" --select {select}"
    cmd += f" --target {dbt_target}"
    trigger_dbt_cli_command(cmd, project_dir="queries", profiles_dir="queries")
