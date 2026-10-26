"""Official materialized reference and transfer configuration safeguards."""

# SQL relations are validated by quote; no user SQL is accepted.
# ruff: noqa: S608
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import re
import time

import google.auth
from google.auth.transport.requests import AuthorizedSession

CONFIG = "projects/37549075344/locations/us/transferConfigs/68e47fbc-0000-27ab-b6f9-883d24faad98"
PUBLIC = "rj-smas-dev.dashboard_acolherio.dev_atendimentos"
CANDIDATE = "rj-smas-dev.dashboard_acolherio.dev_atendimentos_candidata"
DESTINATION = re.compile(
    r"(CREATE\s+OR\s+REPLACE\s+TABLE\s+)(`?)([\w.\-]+)(`?)(\s+AS\s*\()", re.I
)


def version(fingerprint):
    return hashlib.sha256(json.dumps(fingerprint, sort_keys=True).encode()).hexdigest()


def destination(query):
    match = DESTINATION.search(query)
    if not match:
        raise ValueError("Não foi encontrado o destino único da consulta programada.")
    return match.group(3)


def redirect(query):
    if destination(query) not in [PUBLIC, CANDIDATE]:
        raise ValueError("Destino da programada não é o esperado.")
    return DESTINATION.sub(
        lambda m: m.group(1) + "`" + CANDIDATE + "`" + m.group(5), query, count=1
    )


def parity_sql(reference, unified, quote):
    """Validate key sets, ownership, array identity and quantities; zero padding is impossible."""
    r, p = quote(reference), quote(unified)
    return f"""
    ASSERT (SELECT COUNT(*)=0 FROM {r}
      WHERE SEQ_ATENDIMENTO IS NULL OR modulo NOT IN ('u','f') OR modulo IS NULL OR ID_USUARIO IS NULL)
      AS 'Referência contém PK/usuário inválido';
    ASSERT NOT EXISTS (
      SELECT CONCAT(CAST(SEQ_ATENDIMENTO AS STRING),modulo) FROM {r}
      GROUP BY 1 HAVING COUNT(DISTINCT TO_JSON_STRING(STRUCT(ID_USUARIO,DATA_DE_ATENDIMENTO,
        HORA_DE_ATENDIMENTO,UNIDADE_ATENDIMENTO,NOME_ATENDIMENTO_ORIGINAL))) != 1)
      AS 'Referência contém atributos conflitantes para a PK';
    ASSERT (SELECT COUNT(*)=COUNT(DISTINCT usuario.id_usuario) FROM {p})
      AS 'Usuário duplicado na unificada';
    ASSERT (SELECT COUNT(*)=COUNT(DISTINCT a.id_atendimento_modulo)
      FROM {p} p,UNNEST(p.atendimentos.registros) a) AS 'PK duplicada na unificada';
    ASSERT NOT EXISTS (
      WITH r AS (SELECT DISTINCT CONCAT(CAST(SEQ_ATENDIMENTO AS STRING),modulo) chave,
        CAST(ID_USUARIO AS STRING) usuario FROM {r}),
      p AS (SELECT a.id_atendimento_modulo chave,p.usuario.id_usuario usuario
        FROM {p} p,UNNEST(p.atendimentos.registros) a)
      SELECT * FROM (SELECT * FROM r EXCEPT DISTINCT SELECT * FROM p)
      UNION ALL SELECT * FROM (SELECT * FROM p EXCEPT DISTINCT SELECT * FROM r))
      AS 'PKs/usuários divergem entre programada e unificada';
    ASSERT (SELECT COALESCE(SUM(atendimentos.quantidade),0) FROM {p}) =
      (SELECT COUNT(DISTINCT CONCAT(CAST(SEQ_ATENDIMENTO AS STRING),modulo)) FROM {r})
      AS 'Contagem de atendimentos diverge da referência';
    ASSERT NOT EXISTS (SELECT 1 FROM {p}
      WHERE atendimentos.quantidade != ARRAY_LENGTH(atendimentos.registros)
      OR atendimentos.quantidade != atendimentos.quantidade_por_servico.cras + atendimentos.quantidade_por_servico.creas
        + atendimentos.quantidade_por_servico.centro_pop + atendimentos.quantidade_por_servico.outros)
      AS 'Contagem não corresponde aos registros/serviços';
    """


class Producer:
    def __init__(self):
        credentials, _ = google.auth.default(
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        self.session = AuthorizedSession(credentials)
        self.url = "https://bigquerydatatransfer.googleapis.com/v1/" + CONFIG

    def get(self):
        response = self.session.get(self.url, timeout=30)
        response.raise_for_status()
        return response.json()

    def patch(self, fields):
        response = self.session.patch(
            self.url, params={"updateMask": ",".join(fields)}, json=fields, timeout=30
        )
        response.raise_for_status()
        return response.json()

    def active_runs(self):
        response = self.session.get(
            self.url + "/runs", params={"pageSize": 100}, timeout=30
        )
        response.raise_for_status()
        return [
            r
            for r in response.json().get("transferRuns", [])
            if r.get("state") in ["PENDING", "RUNNING"]
        ]

    def assert_routed(self):
        config = self.get()
        if destination(config["params"]["query"]) != CANDIDATE:
            raise ValueError(
                "Programada ainda escreve na tabela pública. Ative a coordenação antes de publicar."
            )
        if any(
            destination(r.get("params", config["params"])["query"]) == PUBLIC
            for r in self.active_runs()
        ):
            raise ValueError(
                "Há execução antiga da programada escrevendo no destino público."
            )

    def stage(self, backup: Path):
        """Only after a ready Prefect deployment; pause old writers before cutover."""
        config = self.get()
        if not backup.exists():
            backup.parent.mkdir(parents=True, exist_ok=True)
            backup.write_text(json.dumps(config, ensure_ascii=False, indent=2))
            backup.chmod(0o600)
        self.patch({"disabled": True})
        deadline = time.monotonic() + 600
        while self.active_runs():
            if time.monotonic() >= deadline:
                raise ValueError(
                    "Jobs antigos não terminaram em dez minutos; a programada permanece pausada."
                )
            print("Aguardando término dos jobs antigos da programada...", flush=True)
            time.sleep(5)
        self.patch(
            {
                "params": {
                    **config["params"],
                    "query": redirect(config["params"]["query"]),
                }
            }
        )
        self.assert_routed()

    def resume(self):
        self.assert_routed()
        self.patch({"disabled": False})
