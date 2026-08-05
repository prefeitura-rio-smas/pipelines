"""Transformação — dbt genérico (tag-driven)."""
import os
from prefect import flow

from pipelines.datalake.transform.dbt.tasks import create_dbt_report, execute_dbt
from pipelines.utils.settings import BaseSettings


@flow(name="Transformação | dbt")
def dbt_transform_flow(select: str | None = None):
    BaseSettings()  # side effect: _configure_auth()
    dbt_target = os.getenv("MODE", "staging")
    args = ["build", "--target", dbt_target, "--project-dir", "queries", "--profiles-dir", "queries"]
    if select:
        args += ["--select", select]
    execution_info = execute_dbt(args)
    create_dbt_report(execution_info, target=dbt_target, select=select)
