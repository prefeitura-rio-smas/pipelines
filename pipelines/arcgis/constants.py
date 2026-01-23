import os
from typing import Dict, Any
from pipelines.utils.settings import BaseSettings

class Settings(BaseSettings):
    """
    Configurações específicas do pipeline Bolsa Família.
    """
    # --- ArcGIS (Credenciais) ---
    SIURB_URL = os.getenv("SIURB_URL")
    SIURB_USER = os.getenv("SIURB_USER")
    SIURB_PWD = os.getenv("SIURB_PWD")

    _env_configs: Dict[str, Dict[str, Any]] = {
        "prod": {
            "project": "rj-smas-dev",
            "bucket": "rj-smas-dev",
            "dataset": "arcgis_raw"
        },
        "staging": {
            "project": "rj-smas-dev",
            "bucket": "rj-smas-dev",
            "dataset": "arcgis_raw"
        } 
    }
            
settings = Settings()
