from typing import Dict, Any
from pipelines.utils.settings import BaseSettings

class Settings(BaseSettings):
    TABLE_ID: str = "folha"
    WAP_TABLE_ID: str = "folha_wap"
    RAW_PATH: str = "raw/bolsa_familia"

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