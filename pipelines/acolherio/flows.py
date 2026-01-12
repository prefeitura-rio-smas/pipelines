from prefect import flow
from pipelines.tasks import run_dbt_models
from pipelines.utils.settings import BasePipelineSettings


@flow(name="dbt_acolherio_rma")
def acolherio_flow() -> None:
    # Apenas instanciar a classe base já configura a autenticação GCP e o MODE
    settings = BasePipelineSettings()
    
    run_dbt_models("stg_filtro_evolucao")
    
if __name__ == "__main__":
    acolherio_flow()
