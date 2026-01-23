# -*- coding: utf-8 -*-
from typing import Dict, Any
from pipelines.utils.settings import BaseSettings

class Settings(BaseSettings):
    """
    Configurações específicas do pipeline AcolherRio.
    """
    # Se houver configurações específicas por ambiente no futuro, adicione aqui em _env_configs
    _env_configs: Dict[str, Dict[str, Any]] = {}

settings = Settings()
