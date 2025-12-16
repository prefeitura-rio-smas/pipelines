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

    # Credencial GCP (Service Account JSON Path)
    GOOGLE_APPLICATION_CREDENTIALS: str | None = None

    # Configuração Pydantic
    model_config = SettingsConfigDict(
        env_file=Path(__file__).parents[2] / ".env", # Tenta achar .env na raiz do repo
        extra="ignore",
        case_sensitive=False,
    )

    @model_validator(mode='after')
    def configure_gcp_settings(self):
        """
        Define as configurações do GCP com base no MODE, se não forem passadas explicitamente.
        """
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

        # Injeção automática da credencial no ambiente global
        if self.GOOGLE_APPLICATION_CREDENTIALS:
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = self.GOOGLE_APPLICATION_CREDENTIALS

        return self

settings = Settings()
