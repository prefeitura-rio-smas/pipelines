# -*- coding: utf-8 -*-
"""Constantes e configuração do espelho cd_cidadao_pac (CadÚnico).

Exceção consciente de bucket (decisão do Leone, 2026-10-21): o CSV é
exportado para `gs://rj-smas-dev/raw/cadunico/` MESMO em produção, porque o
app do prontuário (VPS fora do nosso alcance) lê esse caminho. A tabela
materializada vai para o projeto/dataset do ambiente (prod: `rj-smas`).
"""

from typing import Any, Dict

from pipelines.constants import constants as global_constants
from pipelines.utils.settings import BaseSettings

# Dataset de origem (produção, somente leitura).
SOURCE_DATASET = "rj-smas.protecao_social_cadunico"

# Tabela cujo mês de partição dispara a exportação.
TRIGGER_TABLE = "identificacao_primeira_pessoa"

# Tabelas lidas pelo SQL de exportação → nome do query parameter da partição.
SOURCE_TABLES: Dict[str, str] = {
    "identificacao_primeira_pessoa": "particao_pessoa",
    "identificacao_controle": "particao_controle",
    "contato": "particao_contato",
    "documento_pessoa": "particao_documento",
    "pessoa_deficiencia": "particao_deficiencia",
}

# Detecção de partição nova: lê apenas metadados (~0 B), nunca os dados.
LATEST_PARTITION_SQL = """
SELECT table_name, MAX(partition_id) AS partition_id
FROM `{dataset}.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name IN ({tables})
  AND partition_id NOT IN ('__NULL__', '__UNPARTITIONED__')
GROUP BY table_name
"""

# Resolve a data exata de `data_particao` dentro da partição mais recente de
# cada tabela. Só roda no caminho de exportação (mensal), nunca na detecção.
PARTITION_DATE_SQL = """
SELECT
{selects}
"""

PARTITION_DATE_SELECT = """
  (SELECT MAX(data_particao)
   FROM `{dataset}.{table}`
   WHERE data_particao BETWEEN DATE '{first_day}' AND DATE '{last_day}') AS `{table}`"""


class Settings(BaseSettings):
    """Configuração por ambiente (MODE=prod|staging).

    staging usa projeto, dataset e prefixo GCS próprios: nunca sobrescreve a
    tabela nem o CSV de produção.
    """

    _env_configs: Dict[str, Dict[str, Any]] = {
        "prod": {
            "project": "rj-smas",
            "dataset": "prontuario_eletronico",
            "table_id": "cd_cidadao_pac",
            "bucket": "rj-smas-dev",
            "gcs_prefix": "raw/cadunico",
        },
        "staging": {
            "project": "rj-smas-dev",
            "dataset": "cadunico_espelho_staging",
            "table_id": "cd_cidadao_pac",
            "bucket": "rj-smas-dev",
            "gcs_prefix": "raw/cadunico-staging",
        },
    }

    @property
    def MODE(self) -> str:
        return global_constants().MODE

    @property
    def GCS_PREFIX(self) -> str:
        return self._current_config["gcs_prefix"]

    @property
    def FULL_TABLE_ID(self) -> str:
        return f"{self.GCP_PROJECT}.{self.GCP_DATASET}.{self.TABLE_ID}"

    @property
    def WATERMARK_BLOB(self) -> str:
        """Marcador da última exportação bem-sucedida (idempotência)."""
        return f"{self.GCS_PREFIX}/.last_export"


settings = Settings()
