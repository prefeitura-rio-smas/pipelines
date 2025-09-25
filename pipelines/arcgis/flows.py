# pipeline/flows.py
from pathlib import Path
import yaml
import sys
import subprocess

from .tasks import extract_arcgis, stage_to_parquet, load_to_bigquery

# Caminho para o YAML de configurações de ingestão
CONFIG_PATH = Path(__file__).with_name("pipelines.yaml")
# Diretório do projeto dbt (pasta paralela `queries`)
DBT_PROJECT_DIR = Path(__file__).parent.parent / "../queries"

def incremental_flow() -> None:
    """
    Percorre o YAML e executa:
      1️⃣ Extract   (ArcGIS)
      2️⃣ Stage     (Parquet + timestamp)
      3️⃣ Load      (BigQuery)
      4️⃣ Transform (dbt models gold)
    """
    cfg = yaml.safe_load(CONFIG_PATH.read_text())

    for job in cfg:
        account = job.get("account", "siurb")
        for layer_name, idx in job["layers"].items():
            print(f"↳ Extraindo {job['name']}/{layer_name} (layer {idx})…")

            # 1️⃣ Extract
            df = extract_arcgis(
                feature_id = job["feature_id"],
                account    = account,
                layer      = idx,
                return_geometry = job.get("return_geometry", False),
            )
            if df.empty:
                print("   • Nada a carregar.")
                continue

            # 2️⃣ Stage
            tmp = Path(f"/tmp/{job['name']}_{layer_name}.parquet")
            stage_to_parquet(df, tmp)        # timestamp incluído aqui

            # 3️⃣ Load
            table = f"{job['name']}_{layer_name}_raw"
            load_to_bigquery(tmp, table)
            print(f"   • {len(df):,} linhas → {table}")

    # 4️⃣ Transform (dbt)
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
