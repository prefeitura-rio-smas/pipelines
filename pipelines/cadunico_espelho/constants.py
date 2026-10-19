from typing import Any, Dict

from pipelines.utils.settings import BaseSettings

DATALAKE_MAX_SQL = """
SELECT CAST(MAX(data_particao) AS DATE)
FROM `rj-smas.protecao_social_cadunico.identificacao_primeira_pessoa`
"""

EXPORTED_MAX_SQL = """
SELECT CAST(MAX(data_particao) AS DATE)
FROM `rj-smas-dev.acolherio.cd_cidadao_pac`
"""

SOURCE_DATASET = "rj-smas.protecao_social_cadunico"


class Settings(BaseSettings):
    _env_configs: Dict[str, Dict[str, Any]] = {
        "prod": {
            "project": "rj-smas-dev",
            "bucket": "rj-smas-dev",
            "dataset": "acolherio",
        },
        "staging": {
            "project": "rj-smas-dev",
            "bucket": "rj-smas-dev",
            "dataset": "acolherio",
        },
    }


settings = Settings()
