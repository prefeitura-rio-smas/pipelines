# pipeline/constants.py
from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    # --- ArcGIS ---
    AGOL_URL: str
    AGOL_USER: str
    AGOL_PWD: str
    SIURB_URL: str
    SIURB_USER: str
    SIURB_PWD: str
    AGOL_LAYER_ID: str
    SIURB_LAYER_ID: str

    # --- GCP ---
    GCP_PROJECT: str = "rj-smas-dev"
    GCP_DATASET: str = "arcgis_raw"
    GCS_BUCKET: str  = "rj-smas-dev"

    # ✅ aceita variáveis a mais e ignora
    model_config = SettingsConfigDict(
        env_file      = Path(__file__).parents[1] / ".env",  # ajuste caminho se quiser
        extra         = "ignore",
        case_sensitive=False,
    )

settings = Settings()
