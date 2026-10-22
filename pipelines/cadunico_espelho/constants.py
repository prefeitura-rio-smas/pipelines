# -*- coding: utf-8 -*-
"""Configuração do espelho cd_cidadao_pac (Prontuário Eletrônico).

A transformação vive no dbt (`marts/cadunico/prontuario_eletronico/
cd_cidadao_pac`). O Prefect só decide se há partição nova, roda o `dbt build`,
exporta o CSV e grava o estado.

Exceção consciente de bucket (decisão do Leone, 2026-10-21): o CSV é
exportado para `gs://rj-smas-dev/raw/cadunico/` MESMO em produção, porque o
app do prontuário (VPS fora do nosso alcance) lê esse caminho. O CSV carrega
PII (CPF, NIS, nome, endereço, telefone — ~2,2 M linhas), então staging
escreve em prefixo próprio e nunca toca o prefixo de produção.
"""

from typing import Any, Dict

from pipelines.constants import constants as global_constants
from pipelines.utils.settings import BaseSettings

# Modelo dbt do espelho e prefixo dos arquivos exportados.
MART_MODEL = "cd_cidadao_pac"
EXPORT_FILE_PREFIX = "cd_cidadao_pac"


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
            "dataset": "relatorio",
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


settings = Settings()
