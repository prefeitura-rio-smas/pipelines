"""Poll the official candidate; publish both public tables in one transaction."""

import os
from pathlib import Path
import subprocess
import sys

from prefect import flow, get_run_logger
from prefect.states import Completed

from pipelines.utils.settings import BaseSettings

PROTOCOL = "programada_v2"


@flow(
    name="Planilha unificada | Publicação pareada",
    version=PROTOCOL,
    log_prints=True,
    timeout_seconds=14400,
)
def planilha_unificada_flow(verificar_ambiente: bool = False):
    BaseSettings()
    target = os.getenv("MODE", "staging")
    if target not in ["staging", "dev"]:
        raise ValueError("Publicação pareada habilitada somente em staging.")
    root = Path(__file__).resolve().parents[4]
    operation = root / "queries/operations/planilha_unificada.py"
    if verificar_ambiente:
        from google.cloud import bigquery

        client = bigquery.Client(project="rj-smas-dev")
        for table in [
            "relatorio.planilha_unificada",
            "dashboard_acolherio.dev_atendimentos_candidata",
        ]:
            client.get_table("rj-smas-dev." + table)
        from queries.operations.referencia_programada import Producer

        Producer().get()  # worker must be able to verify the producer routing too
        if not operation.exists():
            raise ValueError("A imagem não contém a operação de publicação pareada.")
        return Completed(message=f"Canário {PROTOCOL} validado em {target}.")
    command = [sys.executable, str(operation), "--target", target, "ciclo"]
    logger = get_run_logger()
    # Fixed command and validated target, without a shell. Stream logs to Prefect.
    with subprocess.Popen(  # noqa: S603 - fixed argv, without shell
        command, cwd=root, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True
    ) as process:
        for line in process.stdout:
            logger.info(line.rstrip())
        if process.wait() != 0:
            raise RuntimeError(
                "Publicação pareada falhou; tabelas públicas mantidas na última geração válida."
            )
    return {"protocol": PROTOCOL, "target": target}
