from datetime import datetime
import json
import re

import prefect
from prefect import task
import requests

from pipelines.arcgis.constants import settings
from pipelines.arcgis.tasks import resolve_arcgis_url
from pipelines.arcgis.utils import _get_arcgis_token, bq_client

# Padrão ISO de data pura (YYYY-MM-DD) — convertida para epoch ms
# porque campos Date clássicos do ArcGIS rejeitam string (esperam timestamp).
# Campos DateOnly aceitam string ISO e REJEITAM epoch ms — por isso a conversão
# é seletiva por nome de coluna.
_ISO_DATE_RE = re.compile(r'^\d{4}-\d{2}-\d{2}$')

# Colunas da camada meta_acordo_resultados_cpf cujo tipo no ArcGIS é Date clássico
# (esriFieldTypeDate). Somente estas recebem epoch ms.
_DATE_CLASSIC_COLUMNS = {
    'data_entrada',
    'data_saida',
    'nascimento_data',
}


def _to_arcgis_value(col_name, value):
    """Converte valores para o formato aceito pelo applyEdits do ArcGIS.
    Campos Date clássicos (esriFieldTypeDate): string ISO YYYY-MM-DD → epoch ms.
    Campos DateOnly (esriFieldTypeDateOnly): mantém string (rejeita epoch ms).
    """
    if value is None:
        return None
    s = str(value).strip()
    if col_name in _DATE_CLASSIC_COLUMNS and _ISO_DATE_RE.match(s):
        try:
            dt = datetime.strptime(s, '%Y-%m-%d')
            return int(dt.timestamp() * 1000)
        except ValueError:
            return value
    return value


@task
def apply_arcgis_feedback(
    item_id: str,
    delta_table: str,
    layer_idx: int = 0,
    dataset: str = "pic"
):
    logger = prefect.get_run_logger()

    client = bq_client()
    project = settings.GCP_PROJECT
    table_id = f"{project}.{dataset}.{delta_table}"

    query = f"SELECT * FROM `{table_id}`"  # noqa: S608
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
                attributes[col] = _to_arcgis_value(col, clean_value)
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
