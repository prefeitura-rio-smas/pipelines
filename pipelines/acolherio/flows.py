from prefect import flow
from pipelines.tasks import run_dbt_models


@flow(name='dbt_acolherio_rma')
def acolherio_pipeline():
    run_dbt_models('stg_filtro_evolucao')

if __name__ == "__main__":
    acolherio_pipeline()