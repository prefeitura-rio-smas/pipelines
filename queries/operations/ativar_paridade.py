"""Read-only legacy canary; activation is disabled for independent dimensions."""

import argparse
import asyncio

from prefect.client.orchestration import get_client
from prefect.deployments import run_deployment
from prefect.settings import get_current_settings

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
        timeout=600,
        poll_interval=5,
    )
    if not run.state.is_completed():
        raise ValueError("Canário do worker falhou; nenhum produtor foi alterado.")
    if not (run.state.message or "").startswith("Canário programada_v2 validado"):
        raise ValueError("Worker não confirmou o protocolo programada_v2.")
    return deployment.id


async def activate():
    if not get_current_settings().api.url:
        await canary()  # fail before any cloud mutation
    raise ValueError(
        "Ativação pareada desabilitada: dimensões independentes usam somente publicação manual, sem alterar a programada."
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
