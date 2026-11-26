"""Transformação — dbt genérico (tag-driven)."""
import os

from prefect import flow
from prefect.runtime import flow_run

from pipelines.datalake.transform.dbt.prontuario import TEST_PATH
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
    if atualizar_prontuario and dbt_target not in ("staging", "prod"):
        raise ValueError("A atualização do prontuário é disponível apenas em staging e prod")
    if publicar_prontuario and not atualizar_prontuario:
        raise ValueError("Selecione a atualização do prontuário antes de habilitar a publicação")
    args = ["build", "--target", dbt_target, "--project-dir", "queries", "--profiles-dir", "queries"]
    if select:
        args += ["--select", select]
    if atualizar_prontuario:
        if dbt_target == "prod" and select:
            # Dependências do Prontuário que não são selecionadas pela tag daily.
            args += [
                "acolherio_atividades_trabalho", "creas_campos", "formularios_datas_ocorrencia",
                "raw_familias_motivos_saida", "raw_usuarios_enderecos", "raw_usuarios_informacoes_medicas",
            ]
        args += ["--exclude", f"path:{TEST_PATH.rstrip('/')}"]
    execution_info = execute_dbt(args)
    create_dbt_report(execution_info, target=dbt_target, select=select)
    if atualizar_prontuario:
        update_prontuario(flow_run.id, publish=publicar_prontuario, target=dbt_target)
