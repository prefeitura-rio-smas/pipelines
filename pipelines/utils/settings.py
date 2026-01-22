import os
import tempfile
from pathlib import Path
from typing import Literal, Dict, Any

from pydantic import model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class BasePipelineSettings(BaseSettings):
    """
    Classe base para configurações de pipeline.
    Gerencia boilerplate de ambiente e autenticação GCP.
    """
    
    # Define o ambiente de execução: "staging" ou "prod"
    MODE: Literal["staging", "prod"] = "staging"

    # Campos GCP comuns preenchidos via env_defaults ou env vars
    GCP_PROJECT: str | None = None
    GCP_DATASET: str | None = None
    GCS_BUCKET: str | None = None
    GCP_STAGING_DATASET: str | None = None

    # Credenciais GCP (Path ou JSON string)
    GOOGLE_APPLICATION_CREDENTIALS: str | None = None
    GCP_CREDENTIALS: str | None = None

    model_config = SettingsConfigDict(
        env_file=Path(__file__).parents[2] / ".env",
        extra="ignore",
        case_sensitive=False,
    )

    @property
    def env_defaults(self) -> Dict[str, Dict[str, Any]]:
        """Sobrescrever na subclasse com os valores hardcoded."""
        return {}

    @model_validator(mode="after")
    def _configure_infrastructure(self):
        mode = self.MODE
        defaults = self.env_defaults.get(mode, {})

        # 1. Aplica defaults se o campo não estiver setado via ENV
        if not self.GCP_PROJECT:
            self.GCP_PROJECT = defaults.get("project")
        if not self.GCS_BUCKET:
            self.GCS_BUCKET = defaults.get("bucket")
        if not self.GCP_DATASET:
            self.GCP_DATASET = defaults.get("dataset")
        if not self.GCP_STAGING_DATASET:
            self.GCP_STAGING_DATASET = defaults.get("staging_dataset")

        # 2. Autenticação GCP
        if self.GCP_CREDENTIALS and self.GCP_CREDENTIALS.strip().startswith("{"):
            with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".json") as f:
                f.write(self.GCP_CREDENTIALS)
                os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = f.name
        elif self.GOOGLE_APPLICATION_CREDENTIALS:
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = self.GOOGLE_APPLICATION_CREDENTIALS
        
        return self
