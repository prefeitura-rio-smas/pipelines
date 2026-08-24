import json

import pandas as pd
import prefect
from prefect import task
import requests

from pipelines.arcgis.constants import settings
from pipelines.arcgis.primeira_infancia_carioca.tasks import _to_arcgis_value
from pipelines.arcgis.utils import _get_arcgis_token, bq_client, resolve_arcgis_url

# Resolve dataset e nomes de tabela por ambiente (MODE)
IS_PROD = settings.GCP_PROJECT == "rj-smas"
DATASET = "pequenos_cariocas" if IS_PROD else "pic"
DEV_TABLE_NAME = "meta_acordo_resultados_cpf" if IS_PROD else "pequenos_cariocas_meta_ar_cpf_dev"
CW_TABLE_NAME = "crosswalk_meta_acordo_resultados_cpf" if IS_PROD else "crosswalk_meta_ar_cpf"


@task
def ensure_crosswalk_table():
    """Cria a tabela crosswalk caso não exista."""
    client = bq_client()
    project = settings.GCP_PROJECT
    dataset = DATASET
    table_id = f"{project}.{dataset}.{CW_TABLE_NAME}"

    try:
        client.get_table(table_id)
    except Exception:
        query = f"""
        CREATE TABLE IF NOT EXISTS `{table_id}` (
            id_membro_familia STRING NOT NULL,
            objectid_arcgis INT64 NOT NULL,
            data_ultima_sinc TIMESTAMP
        )
        """
        client.query(query).result()
        logger = prefect.get_run_logger()
        logger.info(f"Crosswalk table created: {table_id}")


@task
def apply_arcgis_adds(item_id: str, layer_idx: int = 0):
    """
    Insere features novas no ArcGIS e registra os objectid na crosswalk.
    Pessoas elegíveis: data_saida IS NULL (monitoradas) sem objectid na crosswalk.
    """
    logger = prefect.get_run_logger()
    client = bq_client()
    project = settings.GCP_PROJECT
    dev_table = f"{project}.{DATASET}.{DEV_TABLE_NAME}"
    cw_table = f"{project}.{DATASET}.{CW_TABLE_NAME}"

    # 1. Garantir que crosswalk existe
    ensure_crosswalk_table()

    # 2. Buscar pessoas novas (monitoradas na última partição, sem crosswalk)
    query = f"""
        SELECT dev.* FROM `{dev_table}` dev
        WHERE dev.data_particao = (SELECT MAX(data_particao) FROM `{dev_table}`)
          AND NOT EXISTS (
            SELECT 1 FROM `{cw_table}` cw
            WHERE cw.id_membro_familia = dev.id_membro_familia
          )
    """  # noqa: S608 (nomes de tabela vêm de constantes internas)
    rows = client.query(query).to_dataframe()

    if rows.empty:
        logger.info("Nenhum novo ingresso para adicionar no ArcGIS.")
        return 0

    logger.info(f"Adicionando {len(rows)} novas pessoas ao ArcGIS...")

    # 3. Resolver URL e token
    service_url = resolve_arcgis_url(item_id, layer_idx)
    token = _get_arcgis_token()
    edits_url = f"{service_url}/applyEdits"

    # 4. Montar payload de adds
    adds = []
    for _, row in rows.iterrows():
        attrs = {}
        for col in row.index:
            value = row[col]
            # Tratar nulos e valores especiais (mesmo padrão do apply_arcgis_feedback)
            # Colunas internas do BQ que não existem no layer ArcGIS:
            if col.lower() in ("objectid", "data_particao"):
                continue
            # Converte date/datetime para string ISO (json.dumps não serializa date nativo)
            if hasattr(value, 'isoformat'):
                value = value.isoformat()
            clean = value
            if str(value).strip().lower() in ["none", "nan", "null", "", "nat", "<na>"]:
                clean = None
            attrs[col] = clean
        adds.append({"attributes": attrs})

    # 5. Enviar em lotes
    batch_size = 100
    total_added = 0
    inserted = []

    for i in range(0, len(adds), batch_size):
        batch = adds[i : i + batch_size]
        payload = {
            "f": "json",
            "token": token,
            "adds": json.dumps(batch),
        }
        try:
            response = requests.post(edits_url, data=payload, timeout=120)
            response.raise_for_status()
            result = response.json()
            add_results = result.get("addResults", [])
            for idx, r_ in enumerate(add_results):
                if r_.get("success"):
                    total_added += 1
                    batch_idx = r_.get("addParam", idx)
                    id_membro = batch[batch_idx]["attributes"].get("id_membro_familia", "")
                    inserted.append({
                        "objectid_arcgis": int(r_["objectId"]),
                        "id_membro_familia": id_membro,
                    })
                else:
                    logger.warning(f"Falha ao adicionar (indice {idx}): {r_}")
        except Exception as e:
            logger.error(f"Erro no lote {i} (batch size {len(batch)}): {e}")
            try:
                logger.error(f"HTTP status: {response.status_code}, body: {response.text[:500]}")
            except Exception:  # noqa: S110 (log secundário opcional)
                pass

    # 6. Registrar objectids na crosswalk
    if inserted:
        import datetime
        now = datetime.datetime.now(tz=datetime.UTC)
        query_insert = f"""
            INSERT INTO `{cw_table}` (id_membro_familia, objectid_arcgis, data_ultima_sinc)
            VALUES {', '.join(
                f"('{r['id_membro_familia']}', {r['objectid_arcgis']}, TIMESTAMP('{now.isoformat()}'))"
                for r in inserted
            )}
        """  # noqa: S608 (nomes de tabela vêm de constantes internas)
        try:
            client.query(query_insert).result()
            logger.info(f"Registrados {len(inserted)} objectids na crosswalk.")
        except Exception as e:
            logger.error(f"Erro ao atualizar crosswalk: {e}")

    logger.info(f"Total adicionado: {total_added}")
    return total_added


@task
def apply_arcgis_status_sync(item_id: str, layer_idx: int = 0):
    """
    Marca status_monitoramento_cpf='inativo' no ArcGIS para pessoas que estão
    na crosswalk mas não aparecem mais na última partição da _dev.
    Grava também data_saida = última partição em que a pessoa apareceu na _dev
    (somente quando há histórico; sem fallback para a data da rodada, para não
    "flutuar" o data_saida a cada execução).
    """
    logger = prefect.get_run_logger()
    client = bq_client()
    project = settings.GCP_PROJECT
    dev_table = f"{project}.{DATASET}.{DEV_TABLE_NAME}"
    cw_table = f"{project}.{DATASET}.{CW_TABLE_NAME}"
    motivo_table = f"{project}.{DATASET}.motivo_inativo_meta_ar_cpf"

    # 1. Buscar inativos (na crosswalk mas sem registro na última partição da _dev).
    #    LEFT JOIN com o espelho de motivos (alimentado pelo incremental manual,
    #    pois a SA não acessa rj-crm-registry) para levar status_inativo_motivo.
    query = f"""
        SELECT cw.id_membro_familia, cw.objectid_arcgis,
               (SELECT MAX(dev2.data_particao) FROM `{dev_table}` dev2
                WHERE dev2.id_membro_familia = cw.id_membro_familia) AS data_saida,
               m.status_inativo_motivo
        FROM `{cw_table}` cw
        LEFT JOIN `{motivo_table}` m ON cw.id_membro_familia = m.id_membro_familia
        WHERE NOT EXISTS (
            SELECT 1 FROM `{dev_table}` dev
            WHERE dev.id_membro_familia = cw.id_membro_familia
              AND dev.data_particao = (SELECT MAX(data_particao) FROM `{dev_table}`)
        )
    """  # noqa: S608 (nomes de tabela vêm de constantes internas)
    rows = client.query(query).to_dataframe()

    if rows.empty:
        logger.info("Nenhum inativo para atualizar.")
        return 0

    logger.info(f"Marcando {len(rows)} inativos...")

    # 2. Resolver URL
    service_url = resolve_arcgis_url(item_id, layer_idx)
    token = _get_arcgis_token()
    edits_url = f"{service_url}/applyEdits"

    # 3. Montar payload (objectid + status_monitoramento_cpf + data_saida quando houver
    #    histórico + status_inativo_motivo quando o espelho tiver o motivo)
    updates = []
    for _, row in rows.iterrows():
        attrs = {
            "objectid": int(row["objectid_arcgis"]),
            "status_monitoramento_cpf": "inativo",
        }
        if not pd.isna(row["data_saida"]):
            data_saida_iso = pd.Timestamp(row["data_saida"]).strftime("%Y-%m-%d")
            attrs["data_saida"] = _to_arcgis_value("data_saida", data_saida_iso)
        if not pd.isna(row["status_inativo_motivo"]):
            attrs["status_inativo_motivo"] = row["status_inativo_motivo"]
        updates.append({"attributes": attrs})

    # 4. Enviar
    batch_size = 100
    total_updated = 0

    for i in range(0, len(updates), batch_size):
        batch = updates[i : i + batch_size]
        payload = {
            "f": "json",
            "token": token,
            "updates": json.dumps(batch),
        }
        try:
            response = requests.post(edits_url, data=payload, timeout=120)
            response.raise_for_status()
            result = response.json()
            update_results = result.get("updateResults", [])
            total_updated += sum(1 for r_ in update_results if r_.get("success"))
        except Exception as e:
            logger.error(f"Erro no lote {i}: {e}")

    logger.info(f"Total atualizados: {total_updated}")
    return total_updated
