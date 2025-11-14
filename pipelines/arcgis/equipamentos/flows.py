from pathlib import Path
from dotenv import load_dotenv
from prefect import flow

# Carregando variáveis de ambiente do .env no project root
dotenv_path = Path(__file__).parent.parent.parent.parent / '.env'
load_dotenv(dotenv_path=dotenv_path)

from ..tasks import load_arcgis_to_bigquery, run_dbt_models

@flow(name="Equipamentos | Carga ArcGIS")
def equipamentos_flow() -> None:
    """
    Fluxo para carregar dados do ArcGIS para o BigQuery para o produto equipamentos.
    """
    # Configuration for this product
    job_name = "equipamentos"
    feature_id = "134851a668ca407c8f6b4cb2fd8dfdf8"
    account = "siurb"
    return_geometry = False
    batch_size = None
    order_by_field = "None"
    layer_name = "smas"
    layer_idx = 0

    load_arcgis_to_bigquery(
        job_name=job_name,
        layer_name=layer_name,
        feature_id=feature_id,
        layer_idx=layer_idx,
        account=account,
        return_geometry=return_geometry,
        batch_size=batch_size,
        order_by_field=order_by_field,
    )

    run_dbt_models()

if __name__ == "__main__":
    equipamentos_flow()
