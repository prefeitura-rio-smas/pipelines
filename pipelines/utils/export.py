# -*- coding: utf-8 -*-
"""Exportação genérica de tabela BigQuery para o GCS (`EXPORT DATA`).

Sem semântica de domínio: recebe a tabela, o URI de destino e as opções de
formato. Identificadores são validados (`ValueError`) antes de entrar no SQL.
"""

from dataclasses import dataclass
import re

from google.cloud import bigquery

_IDENTIFIER = re.compile(r"^[A-Za-z0-9_-]+$")
_COMPRESSIONS = {"GZIP", "NONE"}

_SQL_TEMPLATE = """
EXPORT DATA OPTIONS (
  uri = '{uri}',
  format = 'CSV',
  compression = '{compression}',
  overwrite = {overwrite},
  header = {header},
  field_delimiter = '{field_delimiter}'
) AS
SELECT *
FROM `{table_id}`
"""


@dataclass(frozen=True)
class ExportResult:
    """Resultado de uma exportação para o GCS."""

    job_id: str
    uri: str
    bytes_processed: int | None


def _validate_table_id(table_id: str) -> str:
    parts = table_id.split(".")
    if len(parts) not in (2, 3):
        raise ValueError(
            f"table_id deve ser 'dataset.tabela' ou 'projeto.dataset.tabela': {table_id!r}"
        )
    if not all(_IDENTIFIER.match(part) for part in parts):
        raise ValueError(f"table_id invalido (use letras, numeros e _): {table_id!r}")
    return table_id


def _validate_uri(uri: str) -> str:
    if not uri.startswith("gs://"):
        raise ValueError(f"uri deve comecar com gs://: {uri!r}")
    if any(caractere in uri for caractere in ("'", '"', "\n", "\r", " ")):
        raise ValueError(f"uri invalida: {uri!r}")
    return uri


def _validate_field_delimiter(field_delimiter: str) -> str:
    if len(field_delimiter) != 1 or field_delimiter in {"'", '"', "\n", "\r"}:
        raise ValueError(f"field_delimiter invalido: {field_delimiter!r}")
    return field_delimiter


def _validate_compression(compression: str) -> str:
    compression = compression.upper()
    if compression not in _COMPRESSIONS:
        raise ValueError(
            f"compression invalida: {compression!r} (use {sorted(_COMPRESSIONS)})"
        )
    return compression


def build_export_sql(
    table_id: str,
    uri: str,
    *,
    overwrite: bool = True,
    header: bool = True,
    compression: str = "GZIP",
    field_delimiter: str = ",",
) -> str:
    """Monta o `EXPORT DATA` para um CSV no GCS (identificadores validados)."""
    return _SQL_TEMPLATE.format(
        uri=_validate_uri(uri),
        table_id=_validate_table_id(table_id),
        compression=_validate_compression(compression),
        overwrite="true" if overwrite else "false",
        header="true" if header else "false",
        field_delimiter=_validate_field_delimiter(field_delimiter),
    )


def export_table_to_gcs(
    client: bigquery.Client,
    table_id: str,
    uri: str,
    *,
    overwrite: bool = True,
    header: bool = True,
    compression: str = "GZIP",
    field_delimiter: str = ",",
) -> ExportResult:
    """Executa o `EXPORT DATA` e devolve job, URI e bytes processados."""
    sql = build_export_sql(
        table_id,
        uri,
        overwrite=overwrite,
        header=header,
        compression=compression,
        field_delimiter=field_delimiter,
    )
    job = client.query(sql)
    job.result()
    return ExportResult(
        job_id=job.job_id,
        uri=uri,
        bytes_processed=job.total_bytes_processed,
    )
