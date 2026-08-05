"""Transformação — dbt genérico (tag-driven)."""
import os
from prefect import flow
from prefect.artifacts import create_markdown_artifact
from pipelines.utils.dbt import mensagem_falha, resumo_markdown, run_dbt
from pipelines.utils.settings import BaseSettings


@flow(name="Transformação | dbt")
def dbt_transform_flow(select: str | None = None):
    BaseSettings()  # side effect: _configure_auth()
    dbt_target = os.getenv("MODE", "staging")
    args = ["build", "--target", dbt_target, "--project-dir", "queries", "--profiles-dir", "queries"]
    if select:
        args += ["--select", select]
    result = run_dbt(args)

    create_markdown_artifact(
        key="dbt-transform-summary",
        description=f"Resumo do dbt build (target={dbt_target}, select={select or 'default'})",
        markdown=resumo_markdown(result),
    )

    if not result.success:
        raise RuntimeError(mensagem_falha(result))
