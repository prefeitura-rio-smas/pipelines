"""Controles da atualização do Prontuário em staging."""
from datetime import datetime, timedelta
from types import SimpleNamespace
from unittest.mock import Mock
from uuid import uuid4

from google.api_core.exceptions import NotFound
import pytest

from pipelines.datalake.transform.dbt import flows, prontuario

NOW = datetime(2026, 11, 24, 10, tzinfo=prontuario.TIMEZONE)


@pytest.mark.parametrize("state,ok", [(None, False), ("RUNNING", True), ("DONE", False)])
def test_carga_indisponivel_bloqueia_leitura_da_origem(monkeypatch, state, ok):
    client = Mock()
    rows = [] if state is None else [SimpleNamespace(job_id="scheduled_query_test", state=state, ok=ok)]
    monkeypatch.setattr(prontuario, "execute", lambda *_args, **_kwargs: rows)

    with pytest.raises(RuntimeError, match="não concluiu"):
        prontuario.scheduled_load(client, NOW, {})

    client.get_table.assert_not_called()


def test_carga_agendada_exige_versao_da_tabela_do_job(monkeypatch):
    job = SimpleNamespace(
        job_id="scheduled_query_test", creation_time=NOW - timedelta(hours=5),
        end_time=NOW - timedelta(hours=5) + timedelta(seconds=15), state="DONE", ok=True,
    )
    client = Mock()
    source = client.get_table.return_value
    source.num_rows = 1
    source.modified = job.end_time + timedelta(seconds=1)
    monkeypatch.setattr(prontuario, "execute", lambda *_args, **_kwargs: [job])

    with pytest.raises(RuntimeError, match="não corresponde"):
        prontuario.scheduled_load(client, NOW, {})

    source.modified = job.end_time
    report = {}
    assert prontuario.scheduled_load(client, NOW, report) is source
    assert report["origem"]["job"] == job.job_id


def test_retry_reanexa_job_sem_reconstruir_ou_publicar_novamente(monkeypatch):
    client = Mock()
    monkeypatch.setattr(prontuario.bigquery, "Client", lambda **_: client)
    finish = Mock()
    monkeypatch.setattr(prontuario, "finish_publication", finish)
    run_id = uuid4()

    prontuario.refresh_prontuario(run_id, publish=True, report={})

    client.get_job.assert_called_once_with(f"prontuario_publish_{run_id.hex}", location="US")
    client.get_job.return_value.result.assert_called_once()
    finish.assert_called_once()
    client.create_dataset.assert_not_called()
    client.query.assert_not_called()


def test_retry_apos_limpeza_reconhece_publicacao_verificada():
    client = Mock()
    client.get_table.side_effect = NotFound("candidata removida")
    client.get_dataset.side_effect = NotFound("dataset removido")
    report = {}

    prontuario.finish_publication(client, "dataset", report)

    assert report["resultado"] == "publicação já verificada e candidata removida"
    client.query.assert_not_called()


def test_publicacao_e_verificacao_usam_projecao_cartoes(monkeypatch):
    client = Mock()
    current = SimpleNamespace(etag="destino", num_rows=2, schema=[])
    client.get_table.return_value = current
    execute = Mock(return_value=[SimpleNamespace(n=0)])
    monkeypatch.setattr(prontuario, "execute", execute)
    report = {}

    prontuario.promote(client, "projeto.dataset", current, "run", "job", report)
    prontuario.finish_publication(client, "projeto.dataset", report)

    _, publication, verification = execute.call_args_list
    assert "SELECT * FROM `projeto.dataset.prontuario_cartoes`" in publication.args[1]
    assert "`projeto.dataset.prontuario_cartoes`" in verification.args[1]
    assert report["resultado"] == "publicada e verificada"


@pytest.mark.parametrize("divergencia", [False, True])
def test_variacao_reconciliada_e_divergencia(monkeypatch, divergencia):
    client = Mock()
    monkeypatch.setattr(prontuario.bigquery, "Client", lambda **_: client)
    client.get_job.side_effect = NotFound("job novo")
    monkeypatch.setattr(prontuario, "scheduled_load", Mock())
    monkeypatch.setattr(prontuario, "materialize", lambda *_: ({}, {}))
    monkeypatch.setattr(prontuario, "compare_volumes", lambda *_: {"novas": 5, "removidas": 2})
    validate = Mock(side_effect=ValueError("divergência") if divergencia else None)
    monkeypatch.setattr(prontuario, "validate", validate)
    prepare = Mock()
    monkeypatch.setattr(prontuario, "prepare_publication", prepare)
    promote = Mock()
    monkeypatch.setattr(prontuario, "promote", promote)
    monkeypatch.setattr(prontuario, "finish_publication", Mock())

    if divergencia:
        with pytest.raises(ValueError, match="divergência"):
            prontuario.refresh_prontuario(uuid4(), publish=True, report={})
        prepare.assert_not_called()
        promote.assert_not_called()
        client.delete_dataset.assert_not_called()
    else:
        prontuario.refresh_prontuario(uuid4(), publish=True, report={})
        prepare.assert_called_once()
        promote.assert_called_once()


def test_atualizacao_do_prontuario_exige_dbt_bem_sucedido_e_staging(monkeypatch):
    monkeypatch.setenv("MODE", "staging")
    monkeypatch.setattr(flows, "BaseSettings", lambda: None)
    monkeypatch.setattr(flows, "execute_dbt", Mock())
    report = Mock(side_effect=RuntimeError("dbt falhou"))
    monkeypatch.setattr(flows, "create_dbt_report", report)
    update = Mock()
    monkeypatch.setattr(flows, "update_prontuario", update)

    with pytest.raises(RuntimeError, match="dbt falhou"):
        flows.dbt_transform_flow.fn("+tag:daily", atualizar_prontuario=True, publicar_prontuario=True)
    update.assert_not_called()

    report.side_effect = None
    run_id = str(uuid4())
    monkeypatch.setattr(flows, "flow_run", SimpleNamespace(id=run_id))
    flows.dbt_transform_flow.fn("+tag:daily", atualizar_prontuario=True)
    update.assert_called_once_with(run_id, publish=False)
    assert flows.execute_dbt.call_args.args[0][-2:] == [
        "--exclude", "path:tests/prontuario_eletronico",
    ]

    monkeypatch.setenv("MODE", "prod")
    with pytest.raises(ValueError, match="staging"):
        flows.dbt_transform_flow.fn("+tag:daily", atualizar_prontuario=True)
