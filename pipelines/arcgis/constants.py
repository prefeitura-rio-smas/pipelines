from typing import Dict, Any
from pipelines.utils.settings import BasePipelineSettings

class Settings(BasePipelineSettings):
    # --- ArcGIS (Credenciais) ---
    SIURB_URL: str
    SIURB_USER: str
    SIURB_PWD: str

    @property
    def env_defaults(self) -> Dict[str, Dict[str, Any]]:
        return {
            "prod": {
                "project": "rj-smas-dev",
                "bucket": "rj-smas-dev",
                "dataset": "arcgis_raw"
            },
            "dev": {
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
