# ruff: noqa: S101
"""Espelho cd_cidadao_pac: regra da competência, export genérico e flow.

Cobre: should_export (nova/igual/ausente), validação de identificador no
EXPORT DATA, leitura da macro (mock), comando/target do dbt, falha no dbt
(sem export/estado), falha no export (estado não avança), mart vazio e
isolamento staging × prod.
"""

from datetime import date
import json
import logging
import types

import pytest

from pipelines.cadunico_bq_to_prontuario_eletronico import flows, tasks, utils
from pipelines.utils import export as export_utils

PARTICAO = date(2026, 9, 11)


@pytest.fixture(autouse=True)
def _silence_logger(monkeypatch):
    logger = logging.getLogger("cadunico-espelho-test")
    monkeypatch.setattr("prefect.get_run_logger", lambda: logger)
    monkeypatch.setattr(
        "pipelines.cadunico_bq_to_prontuario_eletronico.flows.get_run_logger",
        lambda: logger,
    )
    monkeypatch.setattr(
        "pipelines.cadunico_bq_to_prontuario_eletronico.tasks.get_run_logger",
        lambda: logger,
    )
    return logger


# ---------------------------------------------------------------------------
# Regra do produto
# ---------------------------------------------------------------------------


def test_should_export_quando_competencia_nova():
    assert utils.should_export(date(2026, 9, 11), "2026-08") is True


def test_should_export_quando_competencia_igual():
    assert utils.should_export(date(2026, 9, 11), "2026-09") is False


def test_should_export_quando_ausente():
    assert utils.should_export(date(2026, 9, 11), None) is True


def test_should_export_quando_exportada_e_mais_nova():
    assert utils.should_export(date(2026, 8, 7), "2026-09") is False


def test_partition_key_e_caminhos():
    assert utils.partition_key(PARTICAO) == "2026-09"
    assert utils.export_folder(PARTICAO) == "cd_cidadao_pac_2026-09-11"
    assert utils.state_blob(PARTICAO).endswith("/_state/cd_cidadao_pac_2026-09-11.json")
    assert utils.export_uri(PARTICAO).endswith(
        "/cd_cidadao_pac_2026-09-11/cd_cidadao_pac_*.csv.gz"
    )


# ---------------------------------------------------------------------------
# Export genérico (EXPORT DATA)
# ---------------------------------------------------------------------------


def test_build_export_sql_valido():
    sql = export_utils.build_export_sql(
        "rj-smas.prontuario_eletronico.cd_cidadao_pac",
        "gs://bucket/prefixo/cd_cidadao_pac_2026-09-11/cd_cidadao_pac_*.csv.gz",
    )
    assert "EXPORT DATA" in sql
    assert "overwrite = true" in sql
    assert "header = true" in sql
    assert "compression = 'GZIP'" in sql
    assert "`rj-smas.prontuario_eletronico.cd_cidadao_pac`" in sql


@pytest.mark.parametrize(
    "table_id",
    [
        "dataset.tabela; drop table x",
        "dataset.`tabela`",
        "dataset.tabela'",
        "tabela",
    ],
)
def test_build_export_sql_rejeita_table_id_invalido(table_id):
    with pytest.raises(ValueError):
        export_utils.build_export_sql(table_id, "gs://bucket/x_*.csv.gz")


@pytest.mark.parametrize(
    "uri",
    [
        "s3://bucket/x_*.csv.gz",
        "gs://bucket/x_*.csv.gz'",
        "gs://bucket/x y_*.csv.gz",
    ],
)
def test_build_export_sql_rejeita_uri_invalida(uri):
    with pytest.raises(ValueError):
        export_utils.build_export_sql("dataset.tabela", uri)


def test_build_export_sql_rejeita_opcoes_invalidas():
    with pytest.raises(ValueError):
        export_utils.build_export_sql(
            "dataset.tabela", "gs://b/x_*.csv.gz", compression="BZIP2"
        )
    with pytest.raises(ValueError):
        export_utils.build_export_sql(
            "dataset.tabela", "gs://b/x_*.csv.gz", field_delimiter="'"
        )


class _FakeJob:
    job_id = "job-export"
    total_bytes_processed = 123

    def result(self):
        return []


class _FakeBqClient:
    def __init__(self, count=2):
        self.count = count
        self.queries = []

    def query(self, sql):
        self.queries.append(sql)
        if sql.strip().startswith("SELECT COUNT"):
            job = _FakeJob()
            job.result = lambda: [{"n": self.count}]
            return job
        return _FakeJob()


def test_export_table_to_gcs_devolve_resultado():
    client = _FakeBqClient()
    result = export_utils.export_table_to_gcs(
        client, "projeto.dataset.tabela", "gs://b/x_*.csv.gz"
    )
    assert result.job_id == "job-export"
    assert result.bytes_processed == 123
    assert "EXPORT DATA" in client.queries[0]


# ---------------------------------------------------------------------------
# Estado no GCS
# ---------------------------------------------------------------------------


class _FakeBlobObject:
    def __init__(self, name):
        self.name = name


class _FakeBlob:
    def __init__(self, store, name):
        self._store = store
        self._name = name

    def upload_from_string(self, data, content_type=None):
        self._store[self._name] = data


class _FakeBucket:
    def __init__(self, store):
        self._store = store

    def blob(self, name):
        return _FakeBlob(self._store, name)


class _FakeStorageClient:
    def __init__(self, names=()):
        self.names = list(names)
        self.store = {}

    def list_blobs(self, bucket, prefix=""):
        return [_FakeBlobObject(n) for n in self.names if n.startswith(prefix)]

    def bucket(self, name):
        return _FakeBucket(self.store)


def test_exported_dates_le_competencias_dos_estados(monkeypatch):
    client = _FakeStorageClient(
        [
            "raw/cadunico-staging/_state/cd_cidadao_pac_2026-08-07.json",
            "raw/cadunico-staging/_state/cd_cidadao_pac_2026-09-11.json",
            "raw/cadunico-staging/_state/outro.json",
            "raw/cadunico-staging/cd_cidadao_pac_2026-09-11/cd_cidadao_pac_000.csv.gz",
        ]
    )
    monkeypatch.setenv("MODE", "staging")
    assert utils.exported_dates(client, utils.settings.GCS_PREFIX) == [
        "2026-08",
        "2026-09",
    ]


def test_write_state_grava_json(monkeypatch):
    fake = _FakeStorageClient()
    monkeypatch.setattr(tasks, "_storage_client", lambda: fake)
    path = tasks.write_state.fn(PARTICAO, {"rows": 10})
    assert path.endswith("/_state/cd_cidadao_pac_2026-09-11.json")
    assert '"rows": 10' in fake.store[tasks.state_blob(PARTICAO)]


# ---------------------------------------------------------------------------
# dbt
# ---------------------------------------------------------------------------


class _FakeDbtRunner:
    def __init__(self, payload=None, success=True):
        self.payload = payload
        self.success = success
        self.args = None

    def invoke(self, args):
        self.args = args
        if self.success:
            print(json.dumps(self.payload))
        return types.SimpleNamespace(success=self.success, result=None, exception=None)


def test_obter_particao_disponivel_le_json_do_dbt_show(monkeypatch):
    runner = _FakeDbtRunner({"show": [{"data_particao": "2026-09-11"}]})
    monkeypatch.setattr(utils, "dbtRunner", lambda: runner)

    assert utils.obter_particao_disponivel("staging") == PARTICAO

    args = runner.args
    assert args[0] == "show"
    assert any("obter_particao_cadunico()" in arg for arg in args)
    assert args[args.index("--target") + 1] == "staging"
    assert args[args.index("--project-dir") + 1] == "queries"
    assert args[args.index("--profiles-dir") + 1] == "queries"
    assert args[args.index("--output") + 1] == "json"
    assert "--quiet" in args


def test_obter_particao_disponivel_sem_linha_retorna_none(monkeypatch):
    monkeypatch.setattr(utils, "dbtRunner", lambda: _FakeDbtRunner({"show": []}))
    assert utils.obter_particao_disponivel("staging") is None


def test_obter_particao_disponivel_com_data_nula_retorna_none(monkeypatch):
    monkeypatch.setattr(
        utils,
        "dbtRunner",
        lambda: _FakeDbtRunner({"show": [{"data_particao": None}]}),
    )
    assert utils.obter_particao_disponivel("staging") is None


def test_obter_particao_disponivel_falha_no_dbt(monkeypatch):
    monkeypatch.setattr(utils, "dbtRunner", lambda: _FakeDbtRunner(success=False))
    with pytest.raises(RuntimeError, match="dbt show falhou"):
        utils.obter_particao_disponivel("staging")


def test_fetch_particao_disponivel_delega_para_o_produto(monkeypatch):
    calls = {}

    def fake_obter(target):
        calls["target"] = target
        return PARTICAO

    monkeypatch.setattr(tasks, "obter_particao_disponivel", fake_obter)
    assert tasks.fetch_particao_disponivel.fn("staging") == PARTICAO
    assert calls["target"] == "staging"


def test_run_dbt_build_usa_select_e_target(monkeypatch):
    calls = {}

    def fake_trigger(command, project_dir, profiles_dir):
        calls.update(
            command=command, project_dir=project_dir, profiles_dir=profiles_dir
        )

    monkeypatch.setattr(tasks, "trigger_dbt_cli_command", fake_trigger)
    command = tasks.run_dbt_build.fn("prod")
    assert (
        calls["command"]
        == command
        == ("dbt build --select +cd_cidadao_pac --target prod")
    )
    assert calls["project_dir"] == "queries"
    assert calls["profiles_dir"] == "queries"


# ---------------------------------------------------------------------------
# Orquestração do flow
# ---------------------------------------------------------------------------


def _install_fakes(
    monkeypatch,
    *,
    particao=PARTICAO,
    competencias=(),
    rows=10,
    dbt_fail_times=0,
    export_fail_times=0,
):
    state = {
        "estado": None,
        "exports": 0,
        "dbt_runs": 0,
        "dbt_fail_remaining": dbt_fail_times,
        "export_fail_remaining": export_fail_times,
    }

    def fake_dbt(target):
        state["dbt_runs"] += 1
        if state["dbt_fail_remaining"] > 0:
            state["dbt_fail_remaining"] -= 1
            raise RuntimeError("falha simulada no dbt build")
        return f"dbt build --select +cd_cidadao_pac --target {target}"

    def fake_export(partition_date):
        if state["export_fail_remaining"] > 0:
            state["export_fail_remaining"] -= 1
            raise RuntimeError("falha simulada no EXPORT DATA")
        state["exports"] += 1
        return {
            "job_id": "job-1",
            "uri": utils.export_uri(partition_date),
            "bytes_processed": 1,
            "rows": rows,
        }

    def fake_write_state(partition_date, payload):
        state["estado"] = payload
        return f"gs://fake/_state/cd_cidadao_pac_{partition_date.isoformat()}.json"

    monkeypatch.setattr(flows, "fetch_particao_disponivel", lambda target: particao)
    monkeypatch.setattr(
        flows, "fetch_competencias_exportadas", lambda: list(competencias)
    )
    monkeypatch.setattr(flows, "run_dbt_build", fake_dbt)
    monkeypatch.setattr(flows, "run_export", fake_export)
    monkeypatch.setattr(flows, "write_state", fake_write_state)
    return state


def test_flow_exporta_quando_sem_competencia_exportada(monkeypatch):
    state = _install_fakes(monkeypatch)
    result = flows.cadunico_espelho_flow.fn()
    assert result["status"] == "exported"
    assert state["dbt_runs"] == 1
    assert state["exports"] == 1
    assert state["estado"]["competencia"] == "2026-09"


def test_flow_pula_quando_competencia_igual(monkeypatch):
    state = _install_fakes(monkeypatch, competencias=("2026-09",))
    result = flows.cadunico_espelho_flow.fn()
    assert result["status"] == "skipped"
    assert state["dbt_runs"] == 0
    assert state["exports"] == 0
    assert state["estado"] is None


def test_flow_encerra_sem_particao_disponivel(monkeypatch):
    state = _install_fakes(monkeypatch, particao=None)
    result = flows.cadunico_espelho_flow.fn()
    assert result["status"] == "skipped"
    assert result["data_particao"] is None
    assert state["dbt_runs"] == 0
    assert state["exports"] == 0


def test_flow_exporta_quando_competencia_nova(monkeypatch):
    state = _install_fakes(monkeypatch, competencias=("2026-08",))
    result = flows.cadunico_espelho_flow.fn()
    assert result["status"] == "exported"
    assert state["exports"] == 1


def test_falha_no_dbt_nao_exporta_nem_grava_estado(monkeypatch):
    state = _install_fakes(monkeypatch, dbt_fail_times=1)

    with pytest.raises(RuntimeError, match="dbt build"):
        flows.cadunico_espelho_flow.fn()
    assert state["exports"] == 0
    assert state["estado"] is None

    result = flows.cadunico_espelho_flow.fn()
    assert result["status"] == "exported"
    assert state["dbt_runs"] == 2
    assert state["estado"] is not None


def test_falha_no_export_nao_grava_estado_e_reexporta(monkeypatch):
    state = _install_fakes(monkeypatch, export_fail_times=1)

    with pytest.raises(RuntimeError, match="EXPORT DATA"):
        flows.cadunico_espelho_flow.fn()
    assert state["estado"] is None
    assert state["exports"] == 0

    result = flows.cadunico_espelho_flow.fn()
    assert result["status"] == "exported"
    assert state["exports"] == 1
    assert state["estado"]["data_particao"] == "2026-09-11"


def test_run_export_aborta_com_tabela_vazia_sem_exportar(monkeypatch):
    client = _FakeBqClient(count=0)
    monkeypatch.setattr(tasks, "_bq_client", lambda: client)
    with pytest.raises(RuntimeError, match="tabela vazia"):
        tasks.run_export.fn(PARTICAO)
    assert len(client.queries) == 1
    assert client.queries[0].strip().startswith("SELECT COUNT")


def test_run_export_conta_antes_e_exporta(monkeypatch):
    client = _FakeBqClient(count=2_209_619)
    monkeypatch.setattr(tasks, "_bq_client", lambda: client)
    resultado = tasks.run_export.fn(PARTICAO)
    assert resultado["rows"] == 2_209_619
    assert any("EXPORT DATA" in sql for sql in client.queries)


def test_flow_retorna_metadados_de_observabilidade(monkeypatch):
    _install_fakes(monkeypatch)
    result = flows.cadunico_espelho_flow.fn()
    for key in (
        "mode",
        "mart",
        "table",
        "bucket",
        "prefix",
        "data_particao",
        "competencia",
        "rows",
        "uri",
        "job_id",
        "dbt_command",
        "state_uri",
    ):
        assert result[key] is not None


# ---------------------------------------------------------------------------
# Isolamento staging × prod
# ---------------------------------------------------------------------------


def test_staging_nao_toca_destino_de_prod(monkeypatch):
    monkeypatch.setenv("MODE", "staging")
    uri = utils.export_uri(PARTICAO)
    assert uri.startswith("gs://rj-smas-dev/raw/cadunico-staging/")
    assert "/raw/cadunico/" not in uri
    assert tasks.settings.FULL_TABLE_ID == "rj-smas-dev.relatorio.cd_cidadao_pac"
    assert tasks.settings.GCS_PREFIX == "raw/cadunico-staging"


def test_prod_aponta_para_dataset_e_prefixo_de_prod(monkeypatch):
    monkeypatch.setenv("MODE", "prod")
    uri = utils.export_uri(PARTICAO)
    assert uri.startswith("gs://rj-smas-dev/raw/cadunico/")
    assert "raw/cadunico-staging" not in uri
    assert (
        tasks.settings.FULL_TABLE_ID == "rj-smas.prontuario_eletronico.cd_cidadao_pac"
    )
    assert tasks.settings.GCS_PREFIX == "raw/cadunico"
