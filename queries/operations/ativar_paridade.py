"""Activate paired publication only with a healthy deployed worker; never request credentials in logs."""

import argparse
import asyncio
from pathlib import Path
from uuid import UUID

from prefect.client.orchestration import get_client
from prefect.client.schemas.actions import DeploymentUpdate
from prefect.deployments import run_deployment
from prefect.settings import get_current_settings

from pipelines.utils.settings import BaseSettings

try:
    from .planilha_unificada import Pipeline
    from .referencia_programada import CANDIDATE, PUBLIC, Producer
except ImportError:
    from planilha_unificada import Pipeline
    from referencia_programada import CANDIDATE, PUBLIC, Producer

DEPLOYMENT = "Planilha unificada | Publicação pareada/planilha-unificada-staging"


async def canary():
    if not get_current_settings().api.url:
        raise ValueError(
            "Configure PREFECT_API_URL e a autenticação do ambiente antes da ativação; nenhum produtor foi alterado."
        )
    async with get_client() as client:
        deployment = await client.read_deployment_by_name(DEPLOYMENT)
        if deployment.version != "programada_v2":
            raise ValueError("Deployment não está na versão programada_v2.")
        pool = await client.read_work_pool(deployment.work_pool_name)
        if str(pool.status.value) != "READY":
            raise ValueError("Work pool não está READY; nenhum produtor foi alterado.")
    run = await run_deployment(
        DEPLOYMENT,
        parameters={"verificar_ambiente": True},
        timeout=240,
        poll_interval=5,
    )
    if not run.state.is_completed():
        raise ValueError("Canário do worker falhou; nenhum produtor foi alterado.")
    if not (run.state.message or "").startswith("Canário programada_v2 validado"):
        raise ValueError("Worker não confirmou o protocolo programada_v2.")
    return deployment.id


async def activate():
    if not get_current_settings().api.url:
        await canary()  # raises before any cloud mutation
    BaseSettings()
    pipeline = Pipeline("staging")
    from google.api_core.exceptions import NotFound

    try:
        pipeline.client.get_table(CANDIDATE)
    except NotFound:
        pipeline.client.copy_table(PUBLIC, CANDIDATE).result()
    deployment_id = await canary()
    producer = Producer()
    pipeline.dataset("pu_coordenacao", "coordenacao", create=True)
    try:
        pipeline.read_json("pu_coordenacao", "programada_config_original")
    except NotFound:
        original = producer.get()
        pipeline.write_json(
            "pu_coordenacao",
            "programada_config_original",
            {k: original.get(k) for k in ["name", "params", "schedule", "disabled"]},
        )
    backup = (
        Path(__file__).resolve().parents[1]
        / "target/pu/ativacao/programada-original.json"
    )
    # Only after a successful remote canary may the existing schedule be paused/rerouted.
    producer.stage(backup)
    if not pipeline.state().get("programada", {}).get("referencia_versao"):
        from google.cloud import bigquery

        pipeline.client.copy_table(
            PUBLIC,
            CANDIDATE,
            job_config=bigquery.CopyJobConfig(write_disposition="WRITE_TRUNCATE"),
        ).result()
    await asyncio.to_thread(pipeline.cycle)
    async with get_client() as client:
        await client.update_deployment(
            UUID(str(deployment_id)), deployment=DeploymentUpdate(paused=False)
        )
    producer.resume()
    print(
        "Publicação pareada ativada; programada a cada seis horas e verificação Prefect a cada cinco minutos."
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--checar",
        action="store_true",
        help="Executa somente o canário remoto, sem alterar o produtor.",
    )
    args = parser.parse_args()
    asyncio.run(canary() if args.checar else activate())


if __name__ == "__main__":
    main()
