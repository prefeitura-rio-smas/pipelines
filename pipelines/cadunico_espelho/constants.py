# -*- coding: utf-8 -*-
"""Constantes e configuração do espelho cd_cidadao_pac (CadÚnico).

Arquitetura: a transformação vive no dbt (mart `mart_cadunico__cidadao_pac`,
schema `prontuario_eletronico` em prod e `relatorio` em dev). O Prefect só
decide se há partição nova, roda `dbt build`, exporta o CSV e grava o
watermark.

Exceção consciente de bucket (decisão do Leone, 2026-10-21): o CSV é
exportado para `gs://rj-smas-dev/raw/cadunico/` MESMO em produção, porque o
app do prontuário (VPS fora do nosso alcance) lê esse caminho. O CSV carrega
PII (CPF, NIS, nome, endereço, telefone — ~2,2 M linhas), então staging
escreve em prefixo próprio e nunca toca o prefixo de produção.
"""

from typing import Any, Dict

from pipelines.constants import constants as global_constants
from pipelines.utils.settings import BaseSettings

# Dataset de origem (produção, somente leitura).
SOURCE_DATASET = "rj-smas.protecao_social_cadunico"

# Sinal canônico do macro `filtro_particao_cadunico()`: a partição do espelho
# é o MAX(data_particao) desta tabela (a macro pina a mesma data nas 5 tabelas).
TRIGGER_TABLE = "prefeitura"

# Detecção de partição nova: só metadados — nenhum dado lido (piso de
# cobrança de 10 MB de metadata, contra 1,64 GB/dia do MAX(data_particao)).
LATEST_PARTITION_SQL = """
SELECT MAX(partition_id) AS partition_id
FROM `{dataset}.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name = '{table}'
  AND partition_id NOT IN ('__NULL__', '__UNPARTITIONED__')
"""

# Verificação da partição EFETIVAMENTE materializada pelo dbt (query barata:
# só a coluna data_particao do mart, ~18 MB).
MART_PARTITION_SQL = """
SELECT MAX(data_particao) AS data_particao, COUNT(*) AS n
FROM `{table}`
"""

# dbt: o `+` avança os raws (as views usam filtro_particao_cadunico()).
MART_MODEL = "mart_cadunico__cidadao_pac"
DBT_SELECT = f"+{MART_MODEL}"
DBT_PROJECT_DIR = "queries"
DBT_PROFILES_DIR = "queries"


def dbt_build_command(target: str) -> str:
    """Comando do dbt build do espelho (target = prod|staging)."""
    return f"dbt build --select {DBT_SELECT} --target {target}"


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
            "dbt_target": "prod",
        },
        "staging": {
            "project": "rj-smas-dev",
            "dataset": "relatorio",
            "table_id": "cd_cidadao_pac",
            "bucket": "rj-smas-dev",
            "gcs_prefix": "raw/cadunico-staging",
            "dbt_target": "staging",
        },
    }

    @property
    def MODE(self) -> str:
        return global_constants().MODE

    @property
    def DBT_TARGET(self) -> str:
        return self._current_config["dbt_target"]

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
