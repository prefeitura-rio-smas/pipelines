# pipeline/constants.py
import os
from pathlib import Path
from typing import Literal

from pydantic import model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # --- Mode ---
    # Define o ambiente de execução: 'dev' (local), 'staging' (testes no servidor), 'prod' (produção)
    MODE: Literal["dev", "staging", "prod"] = "dev"

    # --- ArcGIS (Credenciais) ---
    SIURB_URL: str
    SIURB_USER: str
    SIURB_PWD: str

    # --- GCP (Configurados dinamicamente baseados no MODE) ---
    GCP_PROJECT: str | None = None
    GCP_DATASET: str | None = None
    GCS_BUCKET: str | None = None

    # Credencial GCP (Service Account JSON Path ou Conteúdo JSON string)
    GOOGLE_APPLICATION_CREDENTIALS: str | None = None
    GCP_CREDENTIALS: str | None = None

    # Configuração Pydantic
    model_config = SettingsConfigDict(
        env_file=Path(__file__).parents[2] / ".env", # Tenta achar .env na raiz do repo
        extra="ignore",
        case_sensitive=False,
    )

    @model_validator(mode='after')
    def configure_gcp_settings(self):
        """
        Define as configurações do GCP com base no MODE e gerencia autenticação.
        """
        import tempfile

        mode = self.MODE

        # Defaults por ambiente
        defaults = {
            "prod": {
                "project": "rj-smas",
                "bucket": "rj-smas",
                "dataset": "arcgis_raw"
            },
            "staging": {
                "project": "rj-smas-dev",
                "bucket": "rj-smas-dev",
                "dataset": "arcgis_raw"
            },
            "dev": {
                "project": "rj-smas-dev",
                "bucket": "rj-smas-dev",
                "dataset": "arcgis_raw"
            }
        }

        if mode == 'dev':
            defaults['dev']['dataset'] = 'arcgis_raw'

        env_config = defaults[mode]

        if not self.GCP_PROJECT:
            self.GCP_PROJECT = env_config["project"]

        if not self.GCS_BUCKET:
            self.GCS_BUCKET = env_config["bucket"]

        if not self.GCP_DATASET:
            self.GCP_DATASET = env_config["dataset"]

        # --- Lógica de Autenticação Híbrida ---
        # 1. Se recebermos o CONTEÚDO do JSON (cenário Server/CI/Docker)
        if self.GCP_CREDENTIALS and self.GCP_CREDENTIALS.strip().startswith("{"):
            # Cria um arquivo temporário com as credenciais
            with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as f:
                f.write(self.GCP_CREDENTIALS)
                temp_cred_path = f.name
            
            # Aponta a variável de ambiente para esse arquivo
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = temp_cred_path
        
        # 2. Se recebermos um CAMINHO de arquivo (cenário Dev Local com .env)
        elif self.GOOGLE_APPLICATION_CREDENTIALS:
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = self.GOOGLE_APPLICATION_CREDENTIALS
        
        # 3. Se nenhum dos dois, o Google Client tentará usar Application Default Credentials (gcloud auth login)

        return self

settings = Settings()
