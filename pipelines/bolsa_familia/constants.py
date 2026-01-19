from typing import Dict, Any
from pipelines.utils.settings import BasePipelineSettings

class Settings(BasePipelineSettings):
    # --- Bolsa Família ---
    # Adicionar aqui campos específicos caso surjam

    @property
    def env_defaults(self) -> Dict[str, Dict[str, Any]]:
        return {
            "prod": {
                "project": "rj-smas",
                "bucket": "rj-smas",
                "dataset": "bolsa_familia",
            },
            "staging": {
                "project": "rj-smas",
                "bucket": "rj-smas",
                "dataset": "bolsa_familia",
            }
        }

settings = Settings()
