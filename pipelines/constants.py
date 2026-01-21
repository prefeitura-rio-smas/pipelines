# -*- coding: utf-8 -*-
import os
from typing import Literal
from pydantic_settings import BaseSettings

class constants(BaseSettings):
    """
    Configurações globais para as pipelines rj-smas.
    Inspirado no padrão SMS/SMTR, centralizando o controle de ambiente.
    """
    # O Pydantic busca automaticamente uma variável de ambiente chamada 'MODE'.
    # Se não encontrar, o valor padrão será 'staging'.
    MODE: Literal["staging", "prod"] = "staging"

    # Adicione aqui outras constantes globais se necessário (ex: TIMEZONE)
    TIMEZONE: str = "America/Sao_Paulo"
