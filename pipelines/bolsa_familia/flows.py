# pipeline/flows.py
from pathlib import Path
import subprocess

#from .tasks import extract_arcgis, stage_to_parquet, load_to_bigquery
from .tasks import connect_to_cloud_storage, list_bucket_files, files_to_digest, digest_files, unzip_data, clean_dirty,  load_to_bigquery

# Diretório do projeto dbt (pasta paralela `queries`)
DBT_PROJECT_DIR = Path(__file__).parent.parent / "../queries"

def incremental_flow() -> None:
    """
    Percorre o YAML e executa:
      1️⃣ Extract   (Bucket)
      2️⃣ Staging     (Parquet + timestamp)
      3️⃣ Load      (BigQuery)
      4️⃣ Transform (dbt models gold)
    """

    # 1️⃣ Extract - ir até o bucket, listar todos os nomes de arquivos contidos dentro do bucket,verificar quais precisam ser ingeridos e extrair-los
    connect_to_cloud_storage(conexao)
    bucket_files = list_bucket_files(BUCKET_NAME)
    files = files_to_digest(bucket_files)
    digest_files(files)  
    
    # 2️⃣ Staging - dezipar e limpar possíveis sujeiras dos arquivos 
    files_unzip = unzip_data(files)
    data_staged = clean_dirty(files_unzip)

    # 3️⃣ Load
    load_to_bigquery(data_staged)
    
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
