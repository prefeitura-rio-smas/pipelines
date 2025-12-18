# pipeline/constants.py
from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    # --- Bolsa Família ---
    # Não há credenciais específicas do Bolsa Família, usamos as do GCP
    
    # --- GCP ---
    GCP_PROJECT: str = "rj-smas-dev"
    GCP_DATASET: str = "bolsa_familia"
    GCS_BUCKET: str = "rj-smas-dev"

    # ✅ aceita variáveis a mais e ignora
    model_config = SettingsConfigDict(
        env_file      = Path(__file__).parents[1] / "../.env",  # ajuste caminho se quiser
        extra         = "ignore",
        case_sensitive=False,
    )

settings = Settings()