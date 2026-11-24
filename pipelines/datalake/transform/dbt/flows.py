"""Transformação — dbt genérico (tag-driven)."""
import os

from prefect import flow
from prefect.runtime import flow_run

from pipelines.datalake.transform.dbt.tasks import (
    create_dbt_report,
    execute_dbt,
    update_prontuario,
)
from pipelines.utils.settings import BaseSettings


@flow(name="Transformação | dbt")
def dbt_transform_flow(
    select: str | None = None,
    atualizar_prontuario: bool = False,
    publicar_prontuario: bool = False,
):
    BaseSettings()  # side effect: _configure_auth()
    dbt_target = os.getenv("MODE", "staging")
    if atualizar_prontuario and dbt_target != "staging":
        raise ValueError("A atualização do prontuário é exclusiva do deployment de staging")
    if publicar_prontuario and not atualizar_prontuario:
        raise ValueError("Selecione a atualização do prontuário antes de habilitar a publicação")
    args = ["build", "--target", dbt_target, "--project-dir", "queries", "--profiles-dir", "queries"]
    if select:
        args += ["--select", select]
    execution_info = execute_dbt(args)
    create_dbt_report(execution_info, target=dbt_target, select=select)
    if atualizar_prontuario:
        update_prontuario(flow_run.id, publish=publicar_prontuario)
