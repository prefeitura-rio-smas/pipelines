import prefect
from prefect import task
import requests
import json
from pipelines.arcgis.utils import _get_arcgis_token, resolve_arcgis_url, bq_client
from pipelines.arcgis.constants import settings


@task
def ensure_crosswalk_table():
    """Cria a tabela crosswalk_meta_ar_cpf caso não exista."""
    client = bq_client()
    project = settings.GCP_PROJECT
    dataset = "pic"
    table_id = f"{project}.{dataset}.crosswalk_meta_ar_cpf"

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
    Pessoas elegíveis: flag_atual=true sem objectid na crosswalk.
    """
    logger = prefect.get_run_logger()
    client = bq_client()
    project = settings.GCP_PROJECT
    dev_table = f"{project}.pic.pequenos_cariocas_meta_ar_cpf_dev"
    cw_table = f"{project}.pic.crosswalk_meta_ar_cpf"

    # 1. Garantir que crosswalk existe
    ensure_crosswalk_table()

    # 2. Buscar pessoas novas (flag_atual=true, sem crosswalk)
    query = f"""
        SELECT dev.* FROM `{dev_table}` dev
        WHERE dev.flag_atual = true
          AND NOT EXISTS (
            SELECT 1 FROM `{cw_table}` cw
            WHERE cw.id_membro_familia = dev.id_membro_familia
          )
    """
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
            if col.lower() == "objectid":
                continue
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
            for r_ in add_results:
                if r_.get("success"):
                    total_added += 1
                    inserted.append({
                        "objectid_arcgis": int(r_["objectId"]),
                        "id_membro_familia": batch[r_["addParam" if "addParam" in r_ else 0]]["attributes"].get(
                            "id_membro_familia", ""
                        ),
                    })
                else:
                    logger.warning(f"Falha ao adicionar: {r_}")
        except Exception as e:
            logger.error(f"Erro no lote {i}: {e}")

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
        """
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
    Marca status_sinc='egresso' no ArcGIS para pessoas que estão na crosswalk
    mas não aparecem mais com flag_atual=true na _dev.
    """
    logger = prefect.get_run_logger()
    client = bq_client()
    project = settings.GCP_PROJECT
    dev_table = f"{project}.pic.pequenos_cariocas_meta_ar_cpf_dev"
    cw_table = f"{project}.pic.crosswalk_meta_ar_cpf"

    # 1. Buscar egressos
    query = f"""
        SELECT cw.id_membro_familia, cw.objectid_arcgis
        FROM `{cw_table}` cw
        WHERE NOT EXISTS (
            SELECT 1 FROM `{dev_table}` dev
            WHERE dev.id_membro_familia = cw.id_membro_familia
              AND dev.flag_atual = true
        )
    """
    rows = client.query(query).to_dataframe()

    if rows.empty:
        logger.info("Nenhum egresso para atualizar.")
        return 0

    logger.info(f"Marcando {len(rows)} egressos...")

    # 2. Resolver URL
    service_url = resolve_arcgis_url(item_id, layer_idx)
    token = _get_arcgis_token()
    edits_url = f"{service_url}/applyEdits"

    # 3. Montar payload (só objectid + status_sinc)
    updates = []
    for _, row in rows.iterrows():
        updates.append({
            "attributes": {
                "objectid": int(row["objectid_arcgis"]),
                "status_sinc": "egresso",
            }
        })

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
