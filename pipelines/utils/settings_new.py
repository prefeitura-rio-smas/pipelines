# -*- coding: utf-8 -*-
import os
import tempfile
from typing import Dict, Any
from pipelines.constants import constants as global_constants

class BaseSettings:
    """
    Nova classe base para configurações de pipeline (v2).
    
    Substitui a lógica baseada em Pydantic por uma abordagem explícita baseada em dicionários
    controlados pela constante global MODE.
    
    Também gerencia a autenticação GCP quando as credenciais são injetadas via variável de ambiente (CI/CD).
    """
    
    # Dicionário de configuração por ambiente. Deve ser sobrescrito na subclasse.
    _env_configs: Dict[str, Dict[str, Any]] = {}

    def __init__(self):
        self._configure_auth()

    def _configure_auth(self):
        """
        Configura a autenticação do Google Cloud.
        Se receber GCP_CREDENTIALS (conteúdo JSON), cria arquivo temporário para o ADC.
        """
        gcp_credentials = os.getenv("GCP_CREDENTIALS")
        
        if gcp_credentials and gcp_credentials.strip().startswith("{"):
            # Evita criar múltiplos arquivos se já estiver configurado nesta sessão
            if "GOOGLE_APPLICATION_CREDENTIALS" not in os.environ:
                try:
                    # Cria arquivo temporário com as credenciais
                    # O delete=False é necessário para que o arquivo persista para leitura pela lib do Google
                    with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".json") as f:
                        f.write(gcp_credentials)
                        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = f.name
                except Exception as e:
                    print(f"Aviso: Falha ao configurar credenciais GCP a partir da variável de ambiente: {e}")

    @property
    def _current_config(self) -> Dict[str, Any]:
        """Resolve a configuração ativa baseada no MODE global."""
        mode = global_constants().MODE
        # Tenta pegar pelo MODE exato, senão fallback para staging, senão vazio
        return self._env_configs.get(mode, self._env_configs.get("staging", {}))

    @property
    def GCP_PROJECT(self) -> str:
        return self._current_config.get("project")

    @property
    def GCS_BUCKET(self) -> str:
        return self._current_config.get("bucket")

    @property
    def GCP_DATASET(self) -> str:
        return self._current_config.get("dataset")

    @property
    def TABLE_ID(self) -> str | None:
        return self._current_config.get("table_id")
