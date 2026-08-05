"""Transformação — dbt genérico (tag-driven)."""
import os
from prefect import flow
from prefect.client.schemas.objects import State
from prefect.states import StateType
from prefect_dbt.cli.commands import trigger_dbt_cli_command
from pipelines.utils.settings import BaseSettings


@flow(name="Transformação | dbt")
def dbt_transform_flow(select: str | None = None):
    BaseSettings()  # side effect: _configure_auth()
    dbt_target = os.getenv("MODE", "staging")
    cmd = "dbt build"
    if select:
        cmd += f" --select {select}"
    cmd += f" --target {dbt_target}"
    result = trigger_dbt_cli_command(cmd, project_dir="queries", profiles_dir="queries")
    if isinstance(result, State) and result.type == StateType.FAILED:
        raise RuntimeError(f"dbt build falhou: {result.message}")
