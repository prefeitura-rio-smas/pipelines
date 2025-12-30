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
    """
    logger = prefect.get_run_logger()

    if model_name is None:
        logger.info("Nenhum modelo dbt para executar.")
        return None

    logger.info(f"🔄 Executando dbt model: {model_name}...")

    dbt_run_op = DbtCoreOperation(
        commands=[f"dbt run --select {model_name}"],
        project_dir=DBT_PROJECT_DIR,
        profiles_dir=DBT_PROJECT_DIR,
    )

    result = dbt_run_op.run()

    logger.info(f"✅ dbt model {model_name} concluído com sucesso.")
    return result
