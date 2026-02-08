# pipeline/utils.py
from datetime import UTC, datetime
from functools import lru_cache
from typing import Literal

import geopandas as gpd
from google.cloud import bigquery, storage
import pandas as pd
import requests
from shapely.geometry import LineString, Point, Polygon

from .constants import settings


# ---------- ArcGIS ----------
@lru_cache(maxsize=1)
def _get_arcgis_token() -> str:
    """Gets an ArcGIS token for the SIURB account."""
    import prefect
    logger = prefect.get_run_logger()

    url = settings.SIURB_URL
    user = settings.SIURB_USER
    pwd = settings.SIURB_PWD

    token_url = f"{url}/sharing/rest/generateToken"

    params = {
        "username": user,
        "password": pwd,
        "f": "json",
        "referer": url,
    }

    logger.info("Gerando token para conta SIURB...")

    try:
        response = requests.post(token_url, data=params, timeout=30)
        response.raise_for_status()
        token = response.json().get("token")
        if not token:
            raise ValueError("Token não encontrado na resposta.")
        logger.info("Token gerado com sucesso.")
        return token
    except requests.exceptions.RequestException as e:
        logger.error(f"Erro ao gerar token: {e}")
        raise ValueError(f"Erro ao gerar token: {e}")

def _get_arcgis_url() -> str:
    """Get the base URL for the SIURB account."""
    return settings.SIURB_URL

@lru_cache(maxsize=10)
def resolve_arcgis_url(item_id: str, layer_idx: int = None) -> str:
    import prefect
    logger = prefect.get_run_logger()

    base_url = _get_arcgis_url()
    token = _get_arcgis_token()

    item_url = f"{base_url}/sharing/rest/content/items/{item_id}"
    params = {"f": "json", "token": token}

    try:
        response = requests.get(item_url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        service_url = data.get("url")
        item_type = data.get("type")
        
        logger.info(f"Item ID: {item_id} | Type: {item_type} | Base URL: {service_url}")

        if not service_url:
            raise ValueError(f"URL não encontrada para o item: {item_id}")

        # Fallback: Se for Feature Service e não passarem index, tentamos o 0
        if item_type == "Feature Service":
            idx = layer_idx if layer_idx is not None else 0
            return f"{service_url.rstrip('/')}/{idx}"
        
        return service_url.rstrip('/')
    except requests.exceptions.RequestException as e:
        logger.error(f"Erro ao resolver URL do ArcGIS: {e}")
        raise ValueError(f"Erro ao resolver URL do ArcGIS: {e}")

@lru_cache(maxsize=10)
def get_layer_service_url(feature_id: str) -> str:
    """Gets the service URL for a given feature item."""
    import prefect
    logger = prefect.get_run_logger()

    logger.info(f"Buscando Service URL para feature_id: {feature_id}")

    base_url = _get_arcgis_url()
    token = _get_arcgis_token()

    item_url = f"{base_url}/sharing/rest/content/items/{feature_id}"
    params = {"f": "json", "token": token}

    try:
        response = requests.get(item_url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        service_url = data.get("url")
        if not service_url:
            raise ValueError(f"Campo 'url' não encontrado nos detalhes do item para feature_id: {feature_id}")
        logger.info(f"Service URL encontrado: {service_url}")
        return service_url
    except requests.exceptions.RequestException as e:
        logger.error(f"Erro ao buscar detalhes do item: {e}")
        raise ValueError(f"Erro ao buscar detalhes do item: {e}")

def get_feature_layer(feature_id: str, layer: int) -> str:
    """
    Returns the URL for the feature layer instead of the ArcGIS object.
    """
    base_url = _get_arcgis_url()
    # Construct the URL to the specific feature layer
    layer_url = f"{base_url}/sharing/rest/content/items/{feature_id}/layers/{layer}?f=json"
    return layer_url

def fetch_dataframe(
    feature_id: str,
    layer: int,
    where: str = "1=1",
    max_records: int = 5000,
    return_geometry: bool = False,
):
    """Baixa dados e devolve DataFrame using direct API calls."""
    import prefect
    logger = prefect.get_run_logger()

    logger.info(f"Iniciando fetch_dataframe - feature_id: {feature_id}, layer: {layer}")

    # Construct the query URL
    base_url = _get_arcgis_url()
    query_url = f"{base_url}/sharing/rest/content/items/{feature_id}/layers/{layer}/query"

    params = {
        "where": where,
        "outFields": "*",
        "returnGeometry": str(return_geometry).lower(),
        "f": "json",
        "resultRecordCount": max_records,
    }

    logger.info(f"Executando query com URL: {query_url}")
    logger.info(f"Params: {params}")

    try:
        response = requests.get(query_url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"Erro ao conectar com o servidor: {e}")
        raise ValueError(f"Erro ao conectar com o servidor: {e}")

    features = data.get("features", [])
    if not features:
        logger.info("Nenhum dado foi encontrado.")
        return pd.DataFrame()

    logger.info(f"Recebido {len(features)} registros")

    processed_data = []
    for feature in features:
        attributes = feature.get("attributes", {})
        geometry_data = feature.get("geometry", {})

        current_attributes = attributes.copy()

        if return_geometry and geometry_data:
            if geometry_data.get("rings"):  # Polygon
                shell = geometry_data["rings"][0]
                holes = geometry_data["rings"][1:] if len(geometry_data["rings"]) > 1 else []
                polygon = Polygon(shell, holes)
                current_attributes["geometry"] = polygon
                processed_data.append(current_attributes)
            elif "x" in geometry_data and "y" in geometry_data:  # Point
                point = Point(geometry_data.get("x"), geometry_data.get("y"))
                current_attributes["latitude"] = geometry_data.get("y")
                current_attributes["longitude"] = geometry_data.get("x")
                current_attributes["geometry"] = point
                processed_data.append(current_attributes)
            elif geometry_data.get("paths"):  # LineString
                line = LineString(geometry_data["paths"][0])
                current_attributes["geometry"] = line
                processed_data.append(current_attributes)

    if processed_data and return_geometry:
        dataframe = pd.DataFrame(processed_data)
        gdf = gpd.GeoDataFrame(dataframe, geometry='geometry')
        logger.info(f"Geometrias processadas, criado GeoDataFrame com CRS: {gdf.crs}")
        return gdf
    return pd.DataFrame([f.get("attributes", {}) for f in features])

def download_data_from_arcgis_task(
    feature_id: str,
    layer: int,
    where: str = "1=1",
    max_records: int = 5000,
    return_geometry: bool = False,
) -> gpd.GeoDataFrame:
    """
    Baixa dados de um serviço ArcGIS REST usando requisições diretas,
    cria um GeoDataFrame com as coordenadas corretas e o retorna.
    """
    import prefect
    logger = prefect.get_run_logger()

    base_url = _get_arcgis_url()
    url = f"{base_url}/sharing/rest/content/items/{feature_id}/layers/{layer}/query"
    url = url[:-1] if url.endswith("/") else url
    url = url + "/query" if not url.endswith("/query") else url

    logger.info(f"Using url:\n{url}")

    params = {
        "where": where,
        "outFields": "*",
        "returnGeometry": str(return_geometry).lower(),
        "f": "json",
        "resultRecordCount": max_records,
        "resultOffset": 0,
    }

    offset = 0
    all_features = []

    logger.info("Iniciando o download...")
    pages = 0
    while True:
        params["resultOffset"] = offset
        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Erro ao conectar com o servidor: {e}")
            raise ValueError(f"Erro ao conectar com o servidor: {e}")

        features = data.get("features", [])
        if not features:
            logger.info("Busca finalizada.")
            break

        all_features.extend(features)
        offset += len(features)
        pages += 1
        logger.info(f"Página {pages} baixada com {len(features)} registros.")
        if not data.get("exceededTransferLimit", False):
            break

    if not all_features:
        logger.info("Nenhum dado foi encontrado.")
        raise ValueError("Nenhum dado foi encontrado.")

    logger.info(
        f"Download completo!\nTotal de {pages} páginas.\nTotal de {len(all_features)} rows."
    )

    logger.info("Processando dados e criando GeoDataFrame...")

    processed_data = []
    for feature in all_features:
        attributes = feature.get("attributes", {})
        geometry_data = feature.get("geometry", {})

        current_attributes = attributes.copy()

        if geometry_data:
            if geometry_data.get("rings"):
                shell = geometry_data["rings"][0]
                holes = geometry_data["rings"][1:] if len(geometry_data["rings"]) > 1 else []
                polygon = Polygon(shell, holes)
                current_attributes["geometry"] = polygon
                processed_data.append(current_attributes)
            elif "x" in geometry_data and "y" in geometry_data:
                point = Point(geometry_data.get("x"), geometry_data.get("y"))
                current_attributes["latitude"] = geometry_data.get("y")
                current_attributes["longitude"] = geometry_data.get("x")
                current_attributes["geometry"] = point
                processed_data.append(current_attributes)
            elif geometry_data.get("paths"):
                line = LineString(geometry_data["paths"][0])
                current_attributes["geometry"] = line
                processed_data.append(current_attributes)

    dataframe = pd.DataFrame(processed_data)
    logger.info(f"old columns: {list(dataframe.columns)}")

    # Remove columns accents if needed - simplified version, you can expand this
    new_columns = [col.replace("á", "a").replace("é", "e").replace("í", "i").replace("ó", "o").replace("ú", "u") \
                  .replace("à", "a").replace("è", "e").replace("ì", "i").replace("ò", "o").replace("ù", "u") \
                  .replace("ã", "a").replace("õ", "o").replace("ç", "c") for col in dataframe.columns]
    logger.info(f"new columns: {new_columns}")
    dataframe.columns = new_columns

    if 'geometry' in dataframe.columns:
        gdf = gpd.GeoDataFrame(dataframe, geometry='geometry')
        logger.info("Convertendo coordenadas para EPSG:4326 (Lat/Lon)...")
        # Assuming the original CRS is WGS84
        gdf.crs = "EPSG:4326"  # Set original CRS if known
        gdf = gdf.to_crs("EPSG:4326")
        if "latitude" in gdf.columns:
            gdf["latitude"] = gdf.geometry.y
            gdf["longitude"] = gdf.geometry.x
        logger.info("Processo concluído!")
        return gdf
    logger.info("Nenhuma geometria encontrada, retornando DataFrame normal")
    return dataframe

def fetch_features_in_chunks(
    feature_id: str,
    layer: int,
    where: str = "1=1",
    chunk_size: int = 200000,
    return_geometry: bool = False,
    order_by_field: str = None,
):
    """
    Busca features em lotes (chunks) e retorna um gerador de DataFrames.
    """
    import prefect
    logger = prefect.get_run_logger()

    logger.info(f"Iniciando fetch_features_in_chunks - feature_id: {feature_id}, layer: {layer}")

    base_url = _get_arcgis_url()
    url = f"{base_url}/sharing/rest/content/items/{feature_id}/layers/{layer}/query"

    # 1. Obter o número total de registros
    logger.info(f"Obtendo contagem total de registros com where: {where}")
    count_params = {
        "where": where,
        "returnCountOnly": "true",
        "f": "json",
    }

    try:
        response = requests.get(url, params=count_params, timeout=30)
        response.raise_for_status()
        count_data = response.json()
        total_records = count_data.get("count", 0)
        logger.info(f"Total de registros encontrados: {total_records}")
    except Exception as e:
        logger.error(f"Erro ao obter contagem total de registros: {e!s}")
        raise

    # 2. Iterar em chunks
    logger.info(f"Iniciando iteração em chunks, total_records: {total_records}, chunk_size: {chunk_size}")
    for i, offset in enumerate(range(0, total_records, chunk_size)):
        records_to_fetch = min(chunk_size, total_records - offset)
        logger.info(f"Processando chunk {i+1}: offset={offset}, records_to_fetch={records_to_fetch}")

        query_params = {
            "where": where,
            "outFields": "*",
            "returnGeometry": str(return_geometry).lower(),
            "f": "json",
            "resultOffset": offset,
            "resultRecordCount": records_to_fetch,
        }
        if order_by_field:
            query_params["orderByFields"] = order_by_field

        try:
            response = requests.get(url, params=query_params, timeout=60)
            response.raise_for_status()
            data = response.json()

            features = data.get("features", [])
            logger.info(f"Chunk {i+1}: recebido {len(features)} registros")
        except Exception as e:
            logger.error(f"Erro ao consultar chunk {i+1} (offset={offset}): {e!s}")
            raise

        if not features:
            logger.info(f"Chunk {i+1} está vazio, pulando...")
            continue

        processed_data = []
        for feature in features:
            attributes = feature.get("attributes", {})
            geometry_data = feature.get("geometry", {})

            current_attributes = attributes.copy()

            if return_geometry and geometry_data:
                if geometry_data.get("rings"):
                    shell = geometry_data["rings"][0]
                    holes = geometry_data["rings"][1:] if len(geometry_data["rings"]) > 1 else []
                    polygon = Polygon(shell, holes)
                    current_attributes["geometry"] = polygon
                    processed_data.append(current_attributes)
                elif "x" in geometry_data and "y" in geometry_data:
                    point = Point(geometry_data.get("x"), geometry_data.get("y"))
                    current_attributes["latitude"] = geometry_data.get("y")
                    current_attributes["longitude"] = geometry_data.get("x")
                    current_attributes["geometry"] = point
                    processed_data.append(current_attributes)
                elif geometry_data.get("paths"):
                    line = LineString(geometry_data["paths"][0])
                    current_attributes["geometry"] = line
                    processed_data.append(current_attributes)

        if processed_data and return_geometry:
            dataframe = pd.DataFrame(processed_data)
            gdf = gpd.GeoDataFrame(dataframe, geometry='geometry')
            # Set CRS if known - for now we'll assume it's WGS84
            gdf.crs = "EPSG:4326"
            gdf = gdf.to_crs("EPSG:4326")
            if not gdf.empty and "latitude" not in gdf.columns:
                gdf["latitude"] = gdf.geometry.y
                gdf["longitude"] = gdf.geometry.x
            logger.info(f"Yielding geodataframe com {len(gdf)} registros")
            yield gdf
        else:
            dataframe = pd.DataFrame([f.get("attributes", {}) for f in features])
            logger.info(f"Yielding dataframe com {len(dataframe)} registros")
            yield dataframe

# ---------- BigQuery / GCS ----------
@lru_cache
def bq_client() -> bigquery.Client:
    return bigquery.Client(project=settings.GCP_PROJECT)

@lru_cache
def gcs_client() -> storage.Client:
    return storage.Client(project=settings.GCP_PROJECT)

def dataset_ref() -> str:
    return f"{settings.GCP_PROJECT}.{settings.GCP_DATASET}"

# ---------- Outros ----------
def add_timestamp(df: pd.DataFrame, column="timestamp_captura") -> pd.DataFrame:
    """
    Acrescenta coluna com datetime UTC nativo ao DataFrame.
    Retorna a mesma instância (conveniente para encadear).
    """
    df[column] = datetime.now(tz=UTC)
    return df
