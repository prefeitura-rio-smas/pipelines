from typing import Dict, Any
from pipelines.utils.settings_new import BaseSettings

class Settings(BaseSettings):
    """
    Configurações específicas do pipeline Bolsa Família.
    Utiliza a nova base BaseSettings para resolução de ambiente.
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