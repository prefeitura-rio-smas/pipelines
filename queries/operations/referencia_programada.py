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


def parity_sql(reference, unified, quote, direct_counts=False):
    """Validate key sets, ownership, array identity and quantities; zero padding is impossible."""
    r, p = quote(reference), quote(unified)
    count_prefix = (
        "atendimentos.quantidade_"
        if direct_counts
        else "atendimentos.quantidade_por_servico."
    )
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
      OR atendimentos.quantidade != {count_prefix}cras + {count_prefix}creas
        + {count_prefix}centro_pop + {count_prefix}outros)
      AS 'Contagem não corresponde aos registros/serviços';
    """


def reference_membership_sql(project, base_schema, quote):
    """Membership independently reconstructed from a service's frozen reference."""

    def table(name):
        return quote(f"{project}.{base_schema}.{name}")

    classify = "CASE u.tipo_unidade WHEN 'CRAS' THEN 'cras' WHEN 'CREAS' THEN 'creas' WHEN 'Centro POP' THEN 'centro_pop' ELSE 'outros' END"
    return f"""WITH origens AS (
      SELECT DISTINCT id_atendimento_modulo,id_unidade FROM {table("raw_atendimentos_usuarios")}
      UNION DISTINCT SELECT DISTINCT id_atendimento_modulo,id_unidade FROM {table("raw_atendimentos_familias")})
      SELECT DISTINCT CAST(r.ID_USUARIO AS STRING) usuario,'atendimentos' bloco,
        CONCAT(CAST(r.SEQ_ATENDIMENTO AS STRING),r.modulo) chave,{classify} servico
      FROM {table("atendimentos_programada")} r
      LEFT JOIN origens o ON CONCAT(CAST(r.SEQ_ATENDIMENTO AS STRING),r.modulo)=o.id_atendimento_modulo
      LEFT JOIN {table("dim_unidades")} u ON o.id_unidade=u.id_unidade
      UNION DISTINCT
      SELECT CAST(COALESCE(e.id_usuario,e.id_paciente_familia) AS STRING),'evolucoes',
        CONCAT(e.origem_modulo,':',CAST(e.id_evolucao AS STRING)),{classify}
      FROM {table("int_evolucoes")} e
      JOIN {table("dim_usuarios")} c ON COALESCE(e.id_usuario,e.id_paciente_familia)=c.id_usuario
      LEFT JOIN {table("dim_unidades")} u ON e.id_unidade=u.id_unidade
      WHERE e.data_cancelamento IS NULL"""


def independent_parity_sql(
    project, manifest, dimensions, unified, users, quote, direct_counts=False
):
    expected = []
    obtained = []
    for service, info in manifest.items():
        if service not in {"cras", "creas", "centro_pop", "outros"}:
            raise ValueError("Serviço inválido no manifesto")
        expected.append(
            f"SELECT * FROM ({reference_membership_sql(project, info['base_schema'], quote)}) WHERE servico='{service}'"
        )
        for block, key in [
            ("atendimentos", "id_atendimento_modulo"),
            ("evolucoes", "id_evolucao_modulo"),
        ]:
            obtained.append(
                f"SELECT CAST(d.id_usuario AS STRING) usuario,'{block}' bloco,r.{key} chave,'{service}' servico FROM {quote(dimensions[service])} d,UNNEST(d.{block}) r"
            )
    expected_sql = " UNION ALL ".join(expected)
    obtained_sql = " UNION ALL ".join(obtained)
    public_sql = " UNION ALL ".join(
        f"SELECT p.usuario.id_usuario usuario,'{block}' bloco,r.{key} chave,r.tipo_servico servico FROM {quote(unified)} p,UNNEST(p.{block}.registros) r"
        for block, key in [
            ("atendimentos", "id_atendimento_modulo"),
            ("evolucoes", "id_evolucao_modulo"),
        ]
    )
    count_prefix = "quantidade_" if direct_counts else "quantidade_por_servico."
    counter_checks = "\n".join(
        f"""ASSERT NOT EXISTS (SELECT 1 FROM {quote(unified)} p
          WHERE p.{block}.{count_prefix}{service} IS DISTINCT FROM
            (SELECT COUNT(*) FROM UNNEST(p.{block}.registros) r WHERE r.tipo_servico='{service}'))
          AS 'Contagem por serviço diverge dos registros';"""
        for block in ["atendimentos", "evolucoes"]
        for service in manifest
    )
    return f"""
      ASSERT NOT EXISTS (
        WITH esperado AS ({expected_sql}), obtido AS ({obtained_sql})
        SELECT * FROM (SELECT * FROM esperado EXCEPT DISTINCT SELECT * FROM obtido)
        UNION ALL SELECT * FROM (SELECT * FROM obtido EXCEPT DISTINCT SELECT * FROM esperado)
      ) AS 'Dimensão diverge da sua referência por serviço/PK/usuário';
      ASSERT NOT EXISTS (
        WITH dimensoes AS ({obtained_sql}), unificada AS ({public_sql})
        SELECT * FROM (SELECT * FROM dimensoes EXCEPT DISTINCT SELECT * FROM unificada)
        UNION ALL SELECT * FROM (SELECT * FROM unificada EXCEPT DISTINCT SELECT * FROM dimensoes)
      ) AS 'Unificada diverge das dimensões utilizadas';
      ASSERT NOT EXISTS (
        SELECT 1 FROM ({obtained_sql}) d LEFT JOIN {quote(users)} u ON d.usuario=CAST(u.id_usuario AS STRING)
        WHERE u.id_usuario IS NULL
      ) AS 'Usuário de dimensão ausente no cadastro da consolidação';
      ASSERT NOT EXISTS (
        SELECT chave FROM ({obtained_sql}) WHERE bloco='atendimentos' GROUP BY chave HAVING COUNT(*)!=1
      ) AS 'Atendimento duplicado entre dimensões';
      ASSERT NOT EXISTS (
        SELECT usuario,chave FROM ({obtained_sql}) WHERE bloco='evolucoes' GROUP BY usuario,chave HAVING COUNT(*)!=1
      ) AS 'Evolução duplicada entre dimensões para o usuário';
      ASSERT (SELECT COUNT(*)=COUNT(DISTINCT usuario.id_usuario) FROM {quote(unified)}) AS 'Usuário duplicado';
      ASSERT NOT EXISTS (
        SELECT 1 FROM {quote(unified)} WHERE atendimentos.quantidade IS DISTINCT FROM ARRAY_LENGTH(atendimentos.registros)
          OR evolucoes.quantidade IS DISTINCT FROM ARRAY_LENGTH(evolucoes.registros)
      ) AS 'Contagem diverge dos registros';
      ASSERT (SELECT COALESCE(SUM(atendimentos.quantidade+evolucoes.quantidade),0) FROM {quote(unified)})=
        (SELECT COUNT(*) FROM ({obtained_sql})) AS 'Contagem total diverge das dimensões';
      {counter_checks}
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
        query = redirect(config["params"]["query"])
        if destination(config["params"]["query"]) != CANDIDATE:
            self.patch({"params": {**config["params"], "query": query}})
        self.assert_routed()

    def resume(self):
        self.assert_routed()
        self.patch({"disabled": False})
