"""Controles da atualização do Prontuário em staging."""
from types import SimpleNamespace
from unittest.mock import Mock
from uuid import uuid4

from google.api_core.exceptions import NotFound
import pytest

from pipelines.datalake.transform.dbt import flows, prontuario


def test_candidata_sem_job_agendado_usa_selecao_dbt(monkeypatch):
    client = Mock()
    monkeypatch.setattr(prontuario.bigquery, "Client", lambda **_: client)
    materialize = Mock(return_value=({}, {}))
    monkeypatch.setattr(prontuario, "materialize", materialize)
    monkeypatch.setattr(prontuario, "validate", Mock())
    monkeypatch.setattr(prontuario, "validate_publication", Mock())
    monkeypatch.setattr(prontuario, "compare_volumes", Mock())
    run_id = uuid4()
    prefix = f"rj-smas-dev.prontuario_build_{run_id.hex}"
    report = {}

    prontuario.refresh_prontuario(run_id, publish=False, report=report)

    client.get_job.assert_not_called()
    client.query.assert_not_called()
    materialize.assert_called_once_with(client, prefix, report)
    assert report["resultado"] == "candidata validada sem promoção"


@pytest.mark.parametrize("count", [0, 3])
def test_materializacao_usa_planilha_isolada_e_exige_atendimentos(monkeypatch, count):
    client = Mock()
    client.get_table.return_value.num_rows = count
    model = prontuario.ATTENDANCE_SELECTION
    key = f"model.queries.{model}"
    source = "`projeto.dashboard_acolherio.filtro_email_dev`"
    relation = f"`projeto.gerenciamento__dbt.{model}`"
    manifest = {
        "nodes": {
            key: {"name": model, "relation_name": relation, "compiled_code": f"select * from {source}"},
            "model.queries.raw_sheets_filtro_email_prontuario": {"relation_name": "`planilha_raw`"},
        },
        "sources": {"source.queries.dashboard_acolherio.filtro_email_dev": {"relation_name": source}},
    }
    monkeypatch.setattr(prontuario, "compile_graph", lambda: (manifest, [key]))
    monkeypatch.setattr(prontuario, "load_sheet", Mock())
    execute = Mock()
    monkeypatch.setattr(prontuario, "execute", execute)
    prefix = "projeto.dataset"
    report = {}

    if count:
        _, references = prontuario.materialize(client, prefix, report)
        assert references[relation] == f"{prefix}.{model}"
        assert report["origem"] == {"modelo": model, "linhas": count}
    else:
        with pytest.raises(RuntimeError, match="seleção dbt de atendimentos está vazia"):
            prontuario.materialize(client, prefix, report)

    client.get_table.assert_called_once_with(f"{prefix}.{model}")
    assert f"select * from `{prefix}.filtro_email`" in execute.call_args.args[1]


def test_materializa_projecao_dbt_a_partir_da_mart_isolada(monkeypatch):
    mart = f"model.queries.{prontuario.MART}"
    projection = f"model.queries.{prontuario.PUBLISHED_MART}"
    source = "`projeto.dashboard_acolherio.filtro_email_dev`"
    nodes = {}
    for name, sql in (
        (prontuario.MART, f"select * from {source}"),
        (prontuario.PUBLISHED_MART, f"select * from `projeto.relatorio.{prontuario.MART}`"),
    ):
        nodes[f"model.queries.{name}"] = {
            "name": name, "resource_type": "model", "config": {"materialized": "table"},
            "compiled_code": sql, "relation_name": f"`projeto.relatorio.{name}`",
        }
    nodes["model.queries.raw_sheets_filtro_email_prontuario"] = {"relation_name": "`planilha_raw`"}
    manifest = {
        "nodes": nodes, "parent_map": {projection: [mart]},
        "sources": {"source.queries.dashboard_acolherio.filtro_email_dev": {"relation_name": source}},
    }
    monkeypatch.setattr(prontuario, "compile_graph", lambda: (manifest, prontuario.ordered_tables(manifest)))
    monkeypatch.setattr(prontuario, "load_sheet", Mock())
    execute = Mock()
    monkeypatch.setattr(prontuario, "execute", execute)

    prontuario.materialize(Mock(), "projeto.isolado", {})

    create_mart, create_projection = execute.call_args_list
    assert "CREATE TABLE `projeto.isolado.prontuario_eletronico`" in create_mart.args[1]
    assert "CREATE TABLE `projeto.isolado.prontuario_cartoes`" in create_projection.args[1]
    assert "from `projeto.isolado.prontuario_eletronico`" in create_projection.args[1]
    assert create_projection.args[3] == "projecao_cartoes"


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


@pytest.mark.parametrize("publicacao_direta_anterior", [False, True])
def test_publicacao_cartoes_e_verificacao_da_candidata(monkeypatch, publicacao_direta_anterior):
    client = Mock()
    current = SimpleNamespace(etag="destino", num_rows=2, schema=[])
    client.get_table.return_value = current
    execute = Mock(return_value=[SimpleNamespace(n=0)])
    monkeypatch.setattr(prontuario, "execute", execute)
    report = {}

    prontuario.promote(client, "projeto.dataset", current, "run", "job", report)
    if publicacao_direta_anterior:
        client.get_table.side_effect = [NotFound("projeção ausente"), current, current]
    prontuario.finish_publication(client, "projeto.dataset", report)

    _, publication, verification = execute.call_args_list
    assert "SELECT * FROM `projeto.dataset.prontuario_cartoes`" in publication.args[1]
    relation = "prontuario_eletronico" if publicacao_direta_anterior else "prontuario_cartoes"
    assert f"`projeto.dataset.{relation}`" in verification.args[1]
    assert report["resultado"] == "publicada e verificada"


@pytest.mark.parametrize("divergencias", [0, 2])
def test_valida_projecao_e_confere_multiplicidade(monkeypatch, divergencias):
    client = Mock()
    candidate = SimpleNamespace(num_rows=2, schema=[])
    client.get_table.return_value = candidate
    execute = Mock(return_value=[SimpleNamespace(n=divergencias)])
    monkeypatch.setattr(prontuario, "execute", execute)
    report = {}

    if divergencias:
        with pytest.raises(ValueError, match="diverge dos atendimentos"):
            prontuario.validate_publication(client, "projeto.dataset", candidate, report)
    else:
        assert prontuario.validate_publication(client, "projeto.dataset", candidate, report) is candidate

    execute.assert_called_once()
    assert "FROM `projeto.dataset.prontuario_eletronico`" in execute.call_args.args[1]
    assert "FROM `projeto.dataset.prontuario_cartoes`" in execute.call_args.args[1]
    assert "HAVING SUM(sinal) != 0" in execute.call_args.args[1]
    assert report["cartoes"] == {"eventos_divergentes": divergencias}


@pytest.mark.parametrize("divergencia", [False, True])
def test_variacao_reconciliada_e_divergencia(monkeypatch, divergencia):
    client = Mock()
    monkeypatch.setattr(prontuario.bigquery, "Client", lambda **_: client)
    client.get_job.side_effect = NotFound("job novo")
    monkeypatch.setattr(prontuario, "materialize", lambda *_: ({}, {}))
    monkeypatch.setattr(prontuario, "compare_volumes", lambda *_: {"novas": 5, "removidas": 2})
    validate = Mock(side_effect=ValueError("divergência") if divergencia else None)
    monkeypatch.setattr(prontuario, "validate", validate)
    prepare = Mock()
    monkeypatch.setattr(prontuario, "validate_publication", prepare)
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
