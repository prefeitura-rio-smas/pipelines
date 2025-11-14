from pathlib import Path
from dotenv import load_dotenv
from prefect import flow

# Carregando variáveis de ambiente do .env no project root
dotenv_path = Path(__file__).parent.parent.parent / '.env'
load_dotenv(dotenv_path=dotenv_path)

import yaml
import time

from .tasks import load_arcgis_to_bigquery, run_dbt_models

# Caminho para o YAML de configurações de ingestão
CONFIG_PATH = Path(__file__).with_name("data_sources.yaml")



@flow
def incremental_flow() -> None:
    """
    Percorre o YAML e executa a carga de cada layers do ArcGIS para o BigQuery.
    """
    start_time = time.monotonic()
    cfg = yaml.safe_load(CONFIG_PATH.read_text())

    for job in cfg:
        for layer_name, idx in job["layers"].items():
            load_arcgis_to_bigquery(
                job_name=job["name"],
                layer_name=layer_name,
                feature_id=job["feature_id"],
                layer_idx=idx,
                account=job.get("account", "siurb"),
                return_geometry=job.get("return_geometry", False),
                batch_size=job.get("chunk_size"),  # Renomeado para batch_size
                order_by_field=job.get("order_by_field"),
            )

    # Transform (dbt)
    run_dbt_models()

    end_time = time.monotonic()
    duration = end_time - start_time
    minutes, seconds = divmod(duration, 60)
    print(f"\n🏁 Pipeline concluída com sucesso em {int(minutes)}m {int(seconds)}s.")



