import os
import prefect
from prefect import task
from prefect_dbt.cli.commands import DbtCoreOperation

from pathlib import Path


DBT_PROJECT_DIR = Path(__file__).parent.parent / "./queries"


@task
def run_dbt_models(model_name: str = None):
    """
    Executa os modelos do dbt usando a integração prefect-dbt.
    Se um model_name for fornecido, executa apenas esse modelo.
    O target é inferido da variável de ambiente MODE (definida no prefect.yaml).
    """
    logger = prefect.get_run_logger()

    if model_name is None:
        logger.info("Nenhum modelo dbt para executar.")
        return None

    # O MODE governará o target do dbt (dev ou prod)
    dbt_target = os.getenv("MODE", "dev")

    logger.info(f"🔄 Executando dbt model: {model_name} com target: {dbt_target}...")

    dbt_run_op = DbtCoreOperation(
        commands=[f"dbt run --select {model_name} --target {dbt_target}"],
        project_dir=DBT_PROJECT_DIR,
        profiles_dir=DBT_PROJECT_DIR,
    )

    result = dbt_run_op.run()

    logger.info(f"✅ dbt model {model_name} concluído com sucesso.")
    return result
