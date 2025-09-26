from pathlib import Path
from dotenv import load_dotenv

# Carregando variáveis de ambiente do .env no project root
dotenv_path = Path(__file__).parent.parent.parent / '.env'
load_dotenv(dotenv_path=dotenv_path)

import yaml
import sys
import subprocess

from .tasks import load_arcgis_to_bigquery

# Caminho para o YAML de configurações de ingestão
CONFIG_PATH = Path(__file__).with_name("data_sources.yaml")
# Diretório do projeto dbt (pasta paralela `queries`)
DBT_PROJECT_DIR = Path(__file__).parent.parent / "../queries"

def incremental_flow() -> None:
    """
    Percorre o YAML e executa a carga de cada layers do ArcGIS para o BigQuery.
    """
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
            )

    # Transform (dbt)
    print("🔄 Executando dbt models (gold)...")
    result = subprocess.run(
        ["dbt", "run", "--project-dir", str(DBT_PROJECT_DIR)],
        cwd=DBT_PROJECT_DIR,
        capture_output=True,
        text=True,
    )
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise RuntimeError("❌ dbt run falhou")
    print("✅ dbt concluído com sucesso.")

if __name__ == "__main__":
    incremental_flow()
