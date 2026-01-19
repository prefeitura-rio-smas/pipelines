from typing import Dict, Any, ClassVar
from pipelines.utils.settings import BasePipelineSettings

class Settings(BasePipelineSettings):
    # --- Bolsa Família ---

    # Constantes do Pipeline
    TABLE_ID: ClassVar[str] = "folha"
    RAW_PATH: ClassVar[str] = "raw/bolsa_familia"

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
                "bucket": "rj-smas",
                "dataset": "bolsa_familia_staging",
            }
        }

settings = Settings()