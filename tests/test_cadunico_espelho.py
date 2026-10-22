# ruff: noqa: S101
"""Espelho cd_cidadao_pac: helpers, watermark e orquestração do flow.

Cobre: ausência de partição, tabela ausente, falha no `dbt build` (zero export
e watermark não avança), falha no export (próximo run reexporta), partição
materializada divergente, comando/target do dbt e isolamento staging × prod.
"""

from datetime import date
import logging

from google.api_core.exceptions import NotFound
import pytest

from pipelines.cadunico_espelho import flows, tasks
from pipelines.cadunico_espelho.constants import dbt_build_command

TRIGGER_PARTITION = "202609"
PARTITION_DATE = date(2026, 9, 11)


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


def test_export_uri_usa_data_da_particao():
    uri = tasks.export_uri(PARTITION_DATE)
    assert uri.endswith("cd_cidadao_pac_2026-09-11/cd_cidadao_pac_*.csv.gz")
    assert uri.startswith("gs://")


def test_build_export_sql_renderiza_placeholders():
    sql = tasks.build_export_sql(PARTITION_DATE)
    body = "\n".join(
        line for line in sql.splitlines() if not line.lstrip().startswith("--")
    )
    assert "{{" not in sql
    assert "EXPORT DATA" in body
    assert "CREATE OR REPLACE" not in body.upper()
    assert "JOIN" not in body.upper()
    assert tasks.settings.FULL_TABLE_ID in body


def test_dbt_build_command_avanca_o_mart():
    assert dbt_build_command("prod") == (
        "dbt build --select +mart_cadunico__cidadao_pac --target prod"
    )
    assert dbt_build_command("staging").endswith("--target staging")


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
    payload = {"partition_id": TRIGGER_PARTITION, "rows": 10}
    path = tasks.write_watermark.fn(payload)
    assert path.endswith(tasks.settings.WATERMARK_BLOB)
    assert tasks.read_watermark.fn() == payload


def test_watermark_payload_registra_evidencia():
    payload = tasks.watermark_payload(
        TRIGGER_PARTITION,
        PARTITION_DATE,
        42,
        {"job_id": "job-1", "uri": "gs://b/x"},
    )
    assert payload["partition_id"] == TRIGGER_PARTITION
    assert payload["data_particao"] == "2026-09-11"
    assert payload["job_id"] == "job-1"
    assert payload["rows"] == 42


# ---------------------------------------------------------------------------
# dbt build
# ---------------------------------------------------------------------------


def test_run_dbt_build_usa_select_e_target_do_modo(monkeypatch):
    calls = {}

    def fake_trigger(command, project_dir, profiles_dir):
        calls.update(
            command=command, project_dir=project_dir, profiles_dir=profiles_dir
        )
        return "ok"

    monkeypatch.setattr(tasks, "trigger_dbt_cli_command", fake_trigger)
    command = tasks.run_dbt_build.fn()
    assert calls["command"] == command
    assert "+mart_cadunico__cidadao_pac" in calls["command"]
    assert f"--target {tasks.settings.DBT_TARGET}" in calls["command"]
    assert calls["project_dir"] == "queries"
    assert calls["profiles_dir"] == "queries"


# ---------------------------------------------------------------------------
# Orquestração do flow
# ---------------------------------------------------------------------------


def _install_fakes(
    monkeypatch,
    *,
    source_partition=TRIGGER_PARTITION,
    table_exists=True,
    mart_date=PARTITION_DATE,
    mart_rows=10,
    dbt_fail_times=0,
    export_fail_times=0,
):
    state = {
        "watermark": None,
        "exports": 0,
        "dbt_runs": 0,
        "dbt_fail_remaining": dbt_fail_times,
        "fail_remaining": export_fail_times,
    }

    def fake_dbt():
        state["dbt_runs"] += 1
        if state["dbt_fail_remaining"] > 0:
            state["dbt_fail_remaining"] -= 1
            raise RuntimeError("falha simulada no dbt build")
        return "dbt build --select +mart_cadunico__cidadao_pac --target staging"

    def fake_export(location, partition_date):
        if state["fail_remaining"] > 0:
            state["fail_remaining"] -= 1
            raise RuntimeError("falha simulada no EXPORT DATA")
        state["exports"] += 1
        return {
            "job_id": "job-1",
            "bytes_processed": 1,
            "uri": tasks.export_uri(partition_date),
        }

    monkeypatch.setattr(flows, "fetch_source_location", lambda: "US")
    monkeypatch.setattr(
        flows, "fetch_latest_partition", lambda location: source_partition
    )
    monkeypatch.setattr(flows, "read_watermark", lambda: state["watermark"])
    monkeypatch.setattr(flows, "export_table_exists", lambda: table_exists)
    monkeypatch.setattr(flows, "run_dbt_build", fake_dbt)
    monkeypatch.setattr(
        flows, "fetch_mart_partition", lambda location: (mart_date, mart_rows)
    )
    monkeypatch.setattr(flows, "run_export", fake_export)

    def fake_write(payload):
        state["watermark"] = payload
        return "gs://fake/.last_export"

    monkeypatch.setattr(flows, "write_watermark", fake_write)
    return state


def test_flow_encerra_sem_particao_de_origem(monkeypatch):
    state = _install_fakes(monkeypatch, source_partition=None)
    result = flows.cadunico_espelho_flow.fn()
    assert result["status"] == "skipped"
    assert state["dbt_runs"] == 0
    assert state["exports"] == 0


def test_flow_exporta_quando_tabela_ausente(monkeypatch):
    state = _install_fakes(monkeypatch, table_exists=False)
    state["watermark"] = {"partition_id": TRIGGER_PARTITION}
    result = flows.cadunico_espelho_flow.fn()
    assert result["status"] == "exported"
    assert state["dbt_runs"] == 1
    assert state["exports"] == 1


def test_falha_no_dbt_nao_exporta_nem_grava_watermark(monkeypatch):
    state = _install_fakes(monkeypatch, dbt_fail_times=1)

    with pytest.raises(RuntimeError, match="dbt build"):
        flows.cadunico_espelho_flow.fn()
    assert state["watermark"] is None
    assert state["exports"] == 0

    result = flows.cadunico_espelho_flow.fn()
    assert result["status"] == "exported"
    assert state["dbt_runs"] == 2
    assert state["watermark"]["partition_id"] == TRIGGER_PARTITION


def test_falha_no_export_nao_grava_watermark_e_reexporta(monkeypatch):
    state = _install_fakes(monkeypatch, export_fail_times=1)

    with pytest.raises(RuntimeError, match="EXPORT DATA"):
        flows.cadunico_espelho_flow.fn()
    assert state["watermark"] is None
    assert state["exports"] == 0

    result = flows.cadunico_espelho_flow.fn()
    assert result["status"] == "exported"
    assert state["exports"] == 1
    assert state["watermark"]["partition_id"] == TRIGGER_PARTITION
    assert result["watermark_written"] is True

    again = flows.cadunico_espelho_flow.fn()
    assert again["status"] == "skipped"
    assert state["dbt_runs"] == 2
    assert state["exports"] == 1


def test_flow_aborta_se_mart_vazio(monkeypatch):
    state = _install_fakes(monkeypatch, mart_date=None, mart_rows=0)
    with pytest.raises(RuntimeError, match="nao materializou dados"):
        flows.cadunico_espelho_flow.fn()
    assert state["exports"] == 0
    assert state["watermark"] is None


def test_flow_aborta_se_particao_materializada_diverge(monkeypatch):
    state = _install_fakes(monkeypatch, mart_date=date(2026, 8, 7))
    with pytest.raises(RuntimeError, match="diverge da particao-gatilho"):
        flows.cadunico_espelho_flow.fn()
    assert state["exports"] == 0
    assert state["watermark"] is None


def test_flow_retorna_metadados_de_observabilidade(monkeypatch):
    _install_fakes(monkeypatch)
    result = flows.cadunico_espelho_flow.fn()
    for key in (
        "mode",
        "dbt_target",
        "project",
        "dataset",
        "table",
        "bucket",
        "prefix",
        "trigger_table",
        "rows",
        "uri",
        "dbt_command",
    ):
        assert result[key] is not None


# ---------------------------------------------------------------------------
# Isolamento staging × prod
# ---------------------------------------------------------------------------


def test_staging_nao_toca_destino_de_prod(monkeypatch):
    monkeypatch.setenv("MODE", "staging")
    sql = tasks.build_export_sql(PARTITION_DATE)
    assert "rj-smas.prontuario_eletronico" not in sql
    assert "gs://rj-smas-dev/raw/cadunico/" not in sql
    assert "rj-smas-dev.relatorio.cd_cidadao_pac" in sql
    assert "raw/cadunico-staging/cd_cidadao_pac_2026-09-11" in sql
    assert tasks.settings.WATERMARK_BLOB == "raw/cadunico-staging/.last_export"
    assert tasks.settings.DBT_TARGET == "staging"


def test_prod_aponta_para_dataset_e_prefixo_de_prod(monkeypatch):
    monkeypatch.setenv("MODE", "prod")
    sql = tasks.build_export_sql(PARTITION_DATE)
    assert "rj-smas.prontuario_eletronico.cd_cidadao_pac" in sql
    assert "raw/cadunico/cd_cidadao_pac_2026-09-11" in sql
    assert "raw/cadunico-staging" not in sql
    assert tasks.settings.WATERMARK_BLOB == "raw/cadunico/.last_export"
    assert tasks.settings.DBT_TARGET == "prod"
