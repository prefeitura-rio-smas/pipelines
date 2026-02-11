import prefect
from prefect import task
import requests
import json
from pipelines.arcgis.utils import _get_arcgis_token, bq_client
from pipelines.arcgis.tasks import resolve_arcgis_url
from pipelines.arcgis.constants import settings

@task
def apply_arcgis_feedback(
    item_id: str,
    delta_table: str,
    layer_idx: int = 0
):
    logger = prefect.get_run_logger()
    
    client = bq_client()
    project = settings.GCP_PROJECT
    dataset = "pic"
    table_id = f"{project}.{dataset}.{delta_table}"
    
    query = f"SELECT * FROM `{table_id}`"
    df = client.query(query).to_dataframe()
    
    if df.empty:
        logger.info(f"Nenhum registro para atualizar em {delta_table}")
        return 0

    rows = df.to_dict(orient="records")
    base_url = resolve_arcgis_url(item_id, layer_idx)
    token = _get_arcgis_token()
    url = f"{base_url}/applyEdits"
    
    updates = []
    for row in rows:
        attributes = {}
        for col, value in row.items():
            clean_value = value
            if str(value).strip().lower() in ["none", "nan", "null", ""]:
                clean_value = None
            
            if col.lower() == "objectid":
                attributes["objectid"] = int(value)
            else:
                attributes[col] = clean_value
        updates.append({"attributes": attributes})

    batch_size = 100
    total_updated = 0
    
    for i in range(0, len(updates), batch_size):
        batch = updates[i : i + batch_size]
        
        payload = {
            "f": "json",
            "token": token,
            "updates": json.dumps(batch)
        }
        
        response = requests.post(url, data=payload, timeout=60)
        response.raise_for_status()
        
        result = response.json()
        update_results = result.get("updateResults", [])
        total_updated += len([r for r in update_results if r.get("success")])

    logger.info(f"Finalizado: {total_updated} registros atualizados no ArcGIS (Item: {item_id})")
    return total_updated
