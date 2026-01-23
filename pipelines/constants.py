# -*- coding: utf-8 -*-
import os
from typing import Literal
from pydantic_settings import BaseSettings

class constants(BaseSettings):
    """
    Valores constantes gerais para pipelines da rj-smas.
    """
    
    # O Pydantic busca automaticamente uma variável de ambiente chamada 'MODE'.
    # Se não encontrar, o valor padrão será 'staging'.
    MODE: Literal["staging", "prod"] = "staging"

    # DEFAULT TIMEZONE #
    TIMEZONE: str = "America/Sao_Paulo"
