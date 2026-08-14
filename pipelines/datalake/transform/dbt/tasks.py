"""Tasks do domínio dbt (execução e relatório)."""
from datetime import datetime

from prefect import get_run_logger, task
from prefect.artifacts import create_markdown_artifact

from dbt.cli.main import dbtRunner
from dbt.contracts.results import NodeStatus
from pipelines.utils.dbt import Summarizer, mensagem_falha


@task
def execute_dbt(args: list[str]) -> dict:
    """Executa o dbt via dbtRunner e retorna o execution_info."""
    start_time = datetime.now()
    running_result = dbtRunner().invoke(args)
    end_time = datetime.now()
    return {
        "command": " ".join(args),
        "running_result": running_result,
        "execution_time": (end_time - start_time).total_seconds(),
        "start_time": start_time,
        "end_time": end_time,
    }


@task
def create_dbt_report(execution_info: dict, target: str, select: str | None = None) -> None:
    """Monta o report (Summarizer), loga, cria artifact markdown e falha se houver erro."""
    logger = get_run_logger()
    result = execution_info["running_result"]
    results = (result.result.results if result.result else None) or []
    summarizer = Summarizer()

    contagem = {}
    for r in results:
        contagem[r.status.value] = contagem.get(r.status.value, 0) + 1

    general_report = []
    for r in results:
        if r.status == NodeStatus.Fail:
            general_report.append(f"- 🛑 FAIL: {summarizer(r)}")
        elif r.status in (NodeStatus.Error, NodeStatus.RuntimeErr):
            general_report.append(f"- ❌ ERROR: {summarizer(r)}")
        elif r.status == NodeStatus.Warn:
            general_report.append(f"- ⚠️ WARN: {summarizer(r)}")

    md = "# dbt build — Resumo do run\n\n"
    for s in NodeStatus:
        md += f"- {s.value}: {contagem.get(s.value, 0)}\n"
    if general_report:
        md += "\n## Nós não-sucedidos ❌\n\n" + "\n".join(general_report) + "\n"

    create_markdown_artifact(
        key="dbt-transform-summary",
        description=f"Resumo do dbt build (target={target}, select={select or 'default'})",
        markdown=md,
    )

    logger.info("dbt build finalizado em %.1fs (target=%s, select=%s)",
                execution_info["execution_time"], target, select or "default")
    if general_report:
        logger.info("\n" + "\n".join(general_report))

    if not result.success:
        raise RuntimeError(mensagem_falha(result))
