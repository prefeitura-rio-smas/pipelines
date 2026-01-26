from typing import Dict, Any
from pipelines.utils.settings import BaseSettings

class Settings(BaseSettings):
    """
    Configurações específicas do pipeline Bolsa Família.
    """
    # Constantes estáticas do Pipeline
    TABLE_ID: str = "folha"
    RAW_PATH: str = "raw/bolsa_familia"

    # Definições por ambiente
    _env_configs: Dict[str, Dict[str, Any]] = {
        "prod": {
            "project": "rj-smas",
            "bucket": "rj-smas",
            "dataset": "bolsa_familia_staging",
        },
        "staging": {
            "project": "rj-smas-dev",
            "bucket": "rj-smas",
            "dataset": "bolsa_familia_staging",
        }
    }

settings = Settings()