# ruff: noqa: S101
"""Espelho cd_cidadao_pac: helpers, watermark e orquestração do flow.

Cobre os cenários exigidos: tabela ausente, datas iguais, falha no export
(watermark não avança → próximo run reexporta), ausência de partição e
isolamento entre staging e produção.
"""

from datetime import date
import logging

from google.api_core.exceptions import NotFound
import pytest

from pipelines.cadunico_espelho import flows, tasks
from pipelines.cadunico_espelho.constants import SOURCE_TABLES, TRIGGER_TABLE

PARTITIONS = {table: "202609" for table in SOURCE_TABLES}
DATES = {table: date(2026, 9, 11) for table in SOURCE_TABLES}


@pytest.fixture(autouse=True)
def _silence_logger(monkeypatch):
    logger = logging.getLogger("cadunico-espelho-test")
    monkeypatch.setattr("prefect.get_run_logger", lambda: logger)
    monkeypatch.setattr(
        "pipelines.cadunico_espelho.flows.get_run_logger", lambda: logger
    )
    monkeypatch.setattr(
        "pipelines.cadunico_espelho.tasks.get_run_logger", lambda: logger
    )
    return logger


class _FakeBlob:
    def __init__(self, store: dict, name: str):
        self._store = store
        self._name = name

    def download_as_text(self) -> str:
        if self._name not in self._store:
            raise NotFound(f"blob ausente: {self._name}")
        return self._store[self._name]

    def upload_from_string(self, data: str, content_type: str | None = None) -> None:
        self._store[self._name] = data


class _FakeBucket:
    def __init__(self, store: dict):
        self._store = store

    def blob(self, name: str) -> _FakeBlob:
        return _FakeBlob(self._store, name)


class _FakeStorageClient:
    def __init__(self):
        self.store: dict = {}

    def bucket(self, name: str) -> _FakeBucket:
        return _FakeBucket(self.store)


# ---------------------------------------------------------------------------
# Helpers puros
# ---------------------------------------------------------------------------


def test_partition_bounds_primeiro_e_ultimo_dia():
    assert tasks.partition_bounds("202609") == (date(2026, 9, 1), date(2026, 9, 30))
    assert tasks.partition_bounds("202602") == (date(2026, 2, 1), date(2026, 2, 28))
    assert tasks.partition_bounds("202402") == (date(2024, 2, 1), date(2024, 2, 29))


def test_export_uri_usa_data_da_particao_gatilho():
    uri = tasks.export_uri(DATES[TRIGGER_TABLE])
    assert uri.endswith("cd_cidadao_pac_2026-09-11/cd_cidadao_pac_*.csv.gz")
    assert uri.startswith("gs://")


def test_build_export_sql_renderiza_placeholders_e_uma_param_por_tabela():
    sql = tasks.build_export_sql(DATES)
    assert "{{" not in sql
    params = tasks.export_query_parameters(DATES)
    assert [p.name for p in params] == list(SOURCE_TABLES.values())
    for name in SOURCE_TABLES.values():
        assert f"@{name}" in sql


# ---------------------------------------------------------------------------
# Watermark no GCS
# ---------------------------------------------------------------------------


def test_read_watermark_ausente_retorna_none(monkeypatch):
    fake = _FakeStorageClient()
    monkeypatch.setattr(tasks, "_storage_client", lambda: fake)
    assert tasks.read_watermark.fn() is None


def test_read_watermark_ilegivel_retorna_none(monkeypatch):
    fake = _FakeStorageClient()
    monkeypatch.setattr(tasks, "_storage_client", lambda: fake)
    fake.store[tasks.settings.WATERMARK_BLOB] = "{nao-e-json"
    assert tasks.read_watermark.fn() is None


def test_write_e_read_watermark_roundtrip(monkeypatch):
    fake = _FakeStorageClient()
    monkeypatch.setattr(tasks, "_storage_client", lambda: fake)
    payload = {"partition_id": "202609", "rows": 10}
    path = tasks.write_watermark.fn(payload)
    assert path.endswith(tasks.settings.WATERMARK_BLOB)
    assert tasks.read_watermark.fn() == payload


# ---------------------------------------------------------------------------
# Orquestração do flow
# ---------------------------------------------------------------------------


def _install_fakes(
    monkeypatch, *, partitions=PARTITIONS, table_exists=True, export_fail_times=0
):
    state = {"watermark": None, "exports": 0, "fail_remaining": export_fail_times}

    def fake_export(partitions_, dates_, location):
        if state["fail_remaining"] > 0:
            state["fail_remaining"] -= 1
            raise RuntimeError("falha simulada no EXPORT DATA")
        state["exports"] += 1
        return {
            "job_id": "job-1",
            "rows": 10,
            "bytes_processed": 1,
            "uri": tasks.export_uri(dates_[TRIGGER_TABLE]),
            "partitions": partitions_,
            "dates": {k: v.isoformat() for k, v in dates_.items()},
        }

    monkeypatch.setattr(flows, "fetch_source_location", lambda: "US")
    monkeypatch.setattr(flows, "fetch_latest_partitions", lambda location: partitions)
    monkeypatch.setattr(flows, "read_watermark", lambda: state["watermark"])
    monkeypatch.setattr(flows, "export_table_exists", lambda: table_exists)
    monkeypatch.setattr(
        flows, "resolve_partition_dates", lambda p, location: dict(DATES)
    )
    monkeypatch.setattr(flows, "run_export", fake_export)

    def fake_write(payload):
        state["watermark"] = payload
        return "gs://fake/.last_export"

    monkeypatch.setattr(flows, "write_watermark", fake_write)
    return state


def test_flow_encerra_sem_particao_de_origem(monkeypatch):
    state = _install_fakes(monkeypatch, partitions={})
    result = flows.cadunico_espelho_flow.fn()
    assert result["status"] == "skipped"
    assert state["exports"] == 0


def test_flow_exporta_quando_tabela_ausente(monkeypatch):
    state = _install_fakes(monkeypatch, table_exists=False)
    state["watermark"] = {"partition_id": "202609"}
    result = flows.cadunico_espelho_flow.fn()
    assert result["status"] == "exported"
    assert state["exports"] == 1


def test_falha_no_export_nao_grava_watermark_e_reexporta(monkeypatch):
    state = _install_fakes(monkeypatch, export_fail_times=1)

    with pytest.raises(RuntimeError):
        flows.cadunico_espelho_flow.fn()
    assert state["watermark"] is None
    assert state["exports"] == 0

    result = flows.cadunico_espelho_flow.fn()
    assert result["status"] == "exported"
    assert state["exports"] == 1
    assert state["watermark"]["partition_id"] == "202609"
    assert result["watermark_written"] is True

    again = flows.cadunico_espelho_flow.fn()
    assert again["status"] == "skipped"
    assert state["exports"] == 1


def test_flow_retorna_metadados_de_observabilidade(monkeypatch):
    _install_fakes(monkeypatch)
    result = flows.cadunico_espelho_flow.fn()
    for key in (
        "mode",
        "project",
        "dataset",
        "table",
        "bucket",
        "prefix",
        "rows",
        "uri",
    ):
        assert result[key] is not None


def test_watermark_payload_registra_evidencia():
    payload = tasks.watermark_payload(
        "202609",
        DATES,
        {"job_id": "job-1", "rows": 42, "uri": "gs://b/x"},
    )
    assert payload["partition_id"] == "202609"
    assert payload["data_particao"] == "2026-09-11"
    assert payload["job_id"] == "job-1"
    assert payload["rows"] == 42


# ---------------------------------------------------------------------------
# Isolamento staging × prod
# ---------------------------------------------------------------------------


def test_staging_nao_toca_destino_de_prod(monkeypatch):
    monkeypatch.setenv("MODE", "staging")
    sql = tasks.build_export_sql(DATES)
    assert "rj-smas.prontuario_eletronico" not in sql
    assert "gs://rj-smas-dev/raw/cadunico/" not in sql
    assert "rj-smas-dev.cadunico_espelho_staging.cd_cidadao_pac" in sql
    assert "raw/cadunico-staging/cd_cidadao_pac_2026-09-11" in sql
    assert tasks.settings.WATERMARK_BLOB == "raw/cadunico-staging/.last_export"


def test_prod_aponta_para_dataset_e_prefixo_de_prod(monkeypatch):
    monkeypatch.setenv("MODE", "prod")
    sql = tasks.build_export_sql(DATES)
    assert "rj-smas.prontuario_eletronico.cd_cidadao_pac" in sql
    assert "raw/cadunico/cd_cidadao_pac_2026-09-11" in sql
    assert "raw/cadunico-staging" not in sql
    assert tasks.settings.WATERMARK_BLOB == "raw/cadunico/.last_export"
