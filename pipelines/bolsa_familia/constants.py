from typing import Dict, Any
from pipelines.utils.settings import BasePipelineSettings

class Settings(BasePipelineSettings):
    # --- Bolsa Família ---

    # Constantes do Pipeline
    TABLE_ID = "folha"
    RAW_PATH = "raw/bolsa_familia"

    @property
    def env_defaults(self) -> Dict[str, Dict[str, Any]]:
        return {
            "prod": {
                "project": "rj-smas",
                "bucket": "rj-smas",
                "dataset": "bolsa_familia_staging",
            },
            "staging": {
                "project": "rj-smas-dev",
                "bucket": "rj-smas-dev",
                "dataset": "bolsa_familia_staging",
            }
        }

settings = Settings()