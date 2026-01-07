from prefect import flow
from pipelines.tasks import run_dbt_models
from constants import configure_gcp_credentials


@flow(name='dbt_acolherio_rma')
def acolherio_flow() -> None:
    configure_gcp_credentials()
    run_dbt_models('stg_filtro_evolucao')
    
if __name__ == "__main__":
    acolherio_flow()