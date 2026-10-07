"""Regressão da ingestão ArcGIS: erro em HTTP 200 e count=0 suspeito.

Cobre o bug exposto pelo flow `mega-spider` (2026-10-07): o ArcGIS devolveu
erro DENTRO de HTTP 200, o extract tratou como layer vazia e recriou a tabela
raw sem schema, quebrando o dbt.
"""

import logging

import pytest

from pipelines.arcgis import tasks
from pipelines.arcgis.tasks import arcgis_to_bq_schema
from pipelines.arcgis.utils import _raise_for_arcgis_error


def _silence_logger(monkeypatch):
    monkeypatch.setattr("prefect.get_run_logger", lambda: logging.getLogger("test"))


class _FakeResponse:
    def __init__(self, body):
        self._body = body

    def raise_for_status(self):
        return None

    def json(self):
        return self._body


_EXPIRED_PASSWORD_BODY = {
    "error": {
        "code": 400,
        "messageCode": "LLS_0002",
        "message": "User's password has expired",
        "details": [],
    }
}


def test_raise_for_arcgis_error_isolates_portal_error():
    with pytest.raises(ValueError) as exc:
        _raise_for_arcgis_error(_EXPIRED_PASSWORD_BODY, "generateToken")
    message = str(exc.value)
    assert "LLS_0002" in message
    assert "password has expired" in message


def test_raise_for_arcgis_error_allows_valid_body():
    # Não deve levantar para respostas legítimas.
    _raise_for_arcgis_error({"token": "abc"}, "generateToken")
    _raise_for_arcgis_error({"fields": [{"name": "objectid"}]}, "layer info")


def test_arcgis_to_bq_schema_empty_without_fields():
    assert arcgis_to_bq_schema([], return_geometry=False) == []


def test_get_layer_info_fails_on_error_body(monkeypatch):
    _silence_logger(monkeypatch)
    monkeypatch.setattr(tasks, "_get_arcgis_token", lambda: "token")
    monkeypatch.setattr(
        tasks.requests, "get", lambda *a, **k: _FakeResponse(_EXPIRED_PASSWORD_BODY)
    )

    with pytest.raises(ValueError, match="LLS_0002"):
        tasks.get_layer_info.fn(service_url="http://x/FeatureServer/0")


def test_get_layer_info_fails_without_fields(monkeypatch):
    _silence_logger(monkeypatch)
    monkeypatch.setattr(tasks, "_get_arcgis_token", lambda: "token")
    monkeypatch.setattr(
        tasks.requests, "get", lambda *a, **k: _FakeResponse({"spatialReference": {}})
    )

    with pytest.raises(ValueError, match="sem 'fields'"):
        tasks.get_layer_info.fn(service_url="http://x/FeatureServer/0")


def test_get_layer_metadata_fails_on_error_body(monkeypatch):
    _silence_logger(monkeypatch)
    monkeypatch.setattr(tasks, "_get_arcgis_token", lambda: "token")
    monkeypatch.setattr(
        tasks.requests, "get", lambda *a, **k: _FakeResponse(_EXPIRED_PASSWORD_BODY)
    )

    with pytest.raises(ValueError, match="LLS_0002"):
        tasks.get_layer_metadata.fn(service_url="http://x/FeatureServer/0")


def test_get_layer_metadata_fails_without_count(monkeypatch):
    _silence_logger(monkeypatch)
    monkeypatch.setattr(tasks, "_get_arcgis_token", lambda: "token")
    monkeypatch.setattr(
        tasks.requests, "get", lambda *a, **k: _FakeResponse({"foo": 1})
    )

    with pytest.raises(ValueError, match="sem 'count'"):
        tasks.get_layer_metadata.fn(service_url="http://x/FeatureServer/0")


def test_load_aborts_without_schema_and_preserves_table(monkeypatch):
    _silence_logger(monkeypatch)
    monkeypatch.setattr(
        tasks, "resolve_arcgis_url", lambda *a, **k: "http://x/FeatureServer/0"
    )
    monkeypatch.setattr(tasks, "get_layer_info", lambda **k: {"fields": [], "crs": {}})
    monkeypatch.setattr(tasks, "get_layer_metadata", lambda **k: 0)

    calls = {"delete": 0, "create": 0}

    class FakeClient:
        def delete_table(self, *a, **k):
            calls["delete"] += 1

        def create_table(self, *a, **k):
            calls["create"] += 1

    monkeypatch.setattr(tasks, "bq_client", lambda: FakeClient())

    with pytest.raises(ValueError, match="Schema vazio"):
        tasks.load_arcgis_to_bigquery.fn(job_name="x", item_id="1", layer_idx=0)

    # Tabela anterior deve ser preservada (sem delete/create).
    assert calls == {"delete": 0, "create": 0}


def test_load_recreates_empty_table_only_with_schema(monkeypatch):
    """count==0 com schema válido mantém o comportamento legítimo (layer vazia)."""
    _silence_logger(monkeypatch)
    monkeypatch.setattr(
        tasks, "resolve_arcgis_url", lambda *a, **k: "http://x/FeatureServer/0"
    )
    monkeypatch.setattr(
        tasks, "get_layer_info", lambda **k: {"fields": [{"name": "objectid"}], "crs": {}}
    )
    monkeypatch.setattr(tasks, "get_layer_metadata", lambda **k: 0)

    recorded = {}

    class FakeClient:
        def delete_table(self, table_id, not_found_ok=False):
            recorded["deleted"] = table_id

        def create_table(self, table):
            recorded["schema"] = table.schema

    monkeypatch.setattr(tasks, "bq_client", lambda: FakeClient())

    tasks.load_arcgis_to_bigquery.fn(job_name="x", item_id="1", layer_idx=0)

    assert "deleted" in recorded
    assert [field.name for field in recorded["schema"]] == [
        "objectid",
        "timestamp_captura",
    ]
