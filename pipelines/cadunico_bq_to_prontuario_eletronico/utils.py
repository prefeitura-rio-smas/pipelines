# -*- coding: utf-8 -*-
"""Funções puras do espelho cd_cidadao_pac (competência, pastas e estado).

Sem chamadas de BigQuery: só regra do produto (recarga no mesmo mês não
reexporta), montagem de caminhos no GCS e leitura dos estados já gravados.
"""

from datetime import date
import re

from google.cloud import storage

from pipelines.cadunico_bq_to_prontuario_eletronico.constants import (
    EXPORT_FILE_PREFIX,
    settings,
)

STATE_PREFIX = "_state"

_STATE_DATE = re.compile(rf"{EXPORT_FILE_PREFIX}_(\d{{4}}-\d{{2}}-\d{{2}})\.json$")


def partition_key(value: date) -> str:
    """Competência AAAA-MM da partição."""
    return value.strftime("%Y-%m")


def should_export(disponivel: date, exportada: str | None) -> bool:
    """Exporta quando a competência disponível é mais nova que a exportada.

    Recarga corretiva no mesmo mês NÃO reexporta (decisão do Leone,
    2026-10-21) — o pior caso é uma sobrescrita manual.
    """
    if exportada is None:
        return True
    return partition_key(disponivel) > exportada


def export_folder(value: date) -> str:
    """Pasta consumida pelo app (uma por data de partição)."""
    return f"{EXPORT_FILE_PREFIX}_{value.isoformat()}"


def export_uri(value: date) -> str:
    """URI wildcard do CSV.gz da partição (contrato do app do prontuário)."""
    return (
        f"gs://{settings.GCS_BUCKET}/{settings.GCS_PREFIX}/{export_folder(value)}/"
        f"{EXPORT_FILE_PREFIX}_*.csv.gz"
    )


def state_blob(value: date) -> str:
    """Caminho do estado da exportação, fora da pasta consumida pelo app."""
    return (
        f"{settings.GCS_PREFIX}/{STATE_PREFIX}/"
        f"{EXPORT_FILE_PREFIX}_{value.isoformat()}.json"
    )


def exported_dates(client: storage.Client, prefix: str) -> list[str]:
    """Competências (AAAA-MM) já exportadas, lidas dos estados no GCS."""
    competencias = set()
    for blob in client.list_blobs(
        settings.GCS_BUCKET, prefix=f"{prefix}/{STATE_PREFIX}/"
    ):
        match = _STATE_DATE.search(blob.name)
        if match:
            competencias.add(match.group(1)[:7])
    return sorted(competencias)
