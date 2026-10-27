# -*- coding: utf-8 -*-
"""Funções do espelho cd_cidadao_pac (partição, competência, pastas e estado).

O domínio fica todo aqui: qual macro do dbt define a partição disponível,
como interpretar o resultado e a regra do produto (recarga no mesmo mês não
reexporta). As funções puras não chamam BigQuery; só a leitura da partição
executa o dbt e a listagem usa o cliente de Storage recebido.
"""

import contextlib
from datetime import date
import io
import json
import re

from dbt.cli.main import dbtRunner
from google.cloud import storage

from pipelines.cadunico_bq_to_prontuario_eletronico.constants import (
    EXPORT_FILE_PREFIX,
    settings,
)
from pipelines.utils.dbt import mensagem_falha

STATE_PREFIX = "_state"

# Inline que executa a macro existente e devolve a partição do CadÚnico.
# A macro é resolvida pelo próprio dbt — nenhum SQL de negócio no Python.
PARTICAO_CADUNICO_INLINE_SQL = (
    "select '{{ obter_particao_cadunico() }}' as data_particao"
)

_STATE_DATE = re.compile(rf"{EXPORT_FILE_PREFIX}_(\d{{4}}-\d{{2}}-\d{{2}})\.json$")


def obter_particao_disponivel(target: str) -> date | None:
    """Executa a macro `obter_particao_cadunico()` e devolve a data da partição.

    Usa `dbt show --inline`, então quem resolve a macro é o dbt (nenhum SQL de
    negócio vive no Python). O `dbtRunner` não expõe as linhas do `show` no
    `result`; com `--quiet --output json` o stdout contém apenas o JSON do
    resultado, capturado aqui. Sem data na saída, devolve `None`.
    """
    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer):
        result = dbtRunner().invoke(
            [
                "show",
                "--inline",
                PARTICAO_CADUNICO_INLINE_SQL,
                "--target",
                target,
                "--project-dir",
                "queries",
                "--profiles-dir",
                "queries",
                "--output",
                "json",
                "--quiet",
            ]
        )
    if not result.success:
        raise RuntimeError(f"dbt show falhou: {mensagem_falha(result)}")
    try:
        payload = json.loads(buffer.getvalue())
    except json.JSONDecodeError as error:
        raise RuntimeError(
            f"dbt show nao devolveu JSON (stdout={buffer.getvalue()!r})"
        ) from error
    rows = payload.get("show") or []
    if not rows:
        return None
    valor = rows[0].get("data_particao")
    if valor is None or str(valor).strip() in ("", "None"):
        return None
    try:
        return date.fromisoformat(str(valor)[:10])
    except ValueError as error:
        raise RuntimeError(f"particao invalida: {valor!r}") from error


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
