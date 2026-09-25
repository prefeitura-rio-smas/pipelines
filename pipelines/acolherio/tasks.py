"""Atualização protegida por frescor da tabela AcolheRio do Looker."""

from datetime import datetime, timedelta, timezone
from pathlib import Path
from time import sleep
from zoneinfo import ZoneInfo

from google.cloud import bigquery
from prefect import get_run_logger, task

from pipelines.utils.settings import BaseSettings


PROJETO_DESTINO = "rj-smas-dev"
TABELA_DESTINO = f"{PROJETO_DESTINO}.dashboard_acolherio.dev_atendimentos"
TABELAS_FONTE = ("gh_atendimentos", "gh_atend_familia")
FUSO_LOCAL = ZoneInfo("America/Sao_Paulo")
HORAS_CICLO_LOCAL = (0, 6, 12, 18)
TEMPO_MAXIMO_ESPERA_MINUTOS = 120
INTERVALO_VERIFICACAO_SEGUNDOS = 300

CONSULTA_FRESCOR = """
SELECT
  table_id,
  TIMESTAMP_MILLIS(last_modified_time) AS ultima_modificacao
FROM `rj-smas.brutos_acolherio_staging.__TABLES__`
WHERE table_id IN ('gh_atendimentos', 'gh_atend_familia')
"""

CONSULTA_VALIDACAO = f"""
SELECT
  COUNT(*) AS linhas,
  MAX(SAFE_CAST(data_de_atendimento AS DATE)) AS data_maxima
FROM `{TABELA_DESTINO}`
"""


def _inicio_ciclo_local(agora_utc: datetime) -> datetime:
    """Retorna o início (UTC) do ciclo de seis horas corrente em São Paulo."""
    agora_local = agora_utc.astimezone(FUSO_LOCAL)
    hora_ciclo = max(hora for hora in HORAS_CICLO_LOCAL if hora <= agora_local.hour)
    inicio_local = agora_local.replace(
        hour=hora_ciclo, minute=0, second=0, microsecond=0
    )
    return inicio_local.astimezone(timezone.utc)


def _cliente_bigquery() -> bigquery.Client:
    BaseSettings()  # Configura ADC quando GCP_CREDENTIALS é injetada pelo Prefect.
    return bigquery.Client(project=PROJETO_DESTINO)


@task(name="Aguardar sincronização das fontes AcolheRio")
def aguardar_fontes_atualizadas() -> dict[str, str]:
    """Espera as duas tabelas-fonte serem modificadas no ciclo corrente."""
    logger = get_run_logger()
    cliente = _cliente_bigquery()
    inicio_ciclo = _inicio_ciclo_local(datetime.now(timezone.utc))
    prazo = datetime.now(timezone.utc) + timedelta(
        minutes=TEMPO_MAXIMO_ESPERA_MINUTOS
    )

    while True:
        atualizacoes = {
            linha.table_id: linha.ultima_modificacao
            for linha in cliente.query(CONSULTA_FRESCOR).result()
        }
        faltantes = [
            tabela
            for tabela in TABELAS_FONTE
            if tabela not in atualizacoes
            or atualizacoes[tabela] is None
            or atualizacoes[tabela].astimezone(timezone.utc) < inicio_ciclo
        ]

        if not faltantes:
            resumo = {
                tabela: atualizacoes[tabela].astimezone(timezone.utc).isoformat()
                for tabela in TABELAS_FONTE
            }
            logger.info("Fontes atualizadas no ciclo %s: %s", inicio_ciclo, resumo)
            return resumo

        agora = datetime.now(timezone.utc)
        if agora >= prazo:
            raise TimeoutError(
                "As fontes AcolheRio não atualizaram no ciclo "
                f"{inicio_ciclo.isoformat()} dentro de "
                f"{TEMPO_MAXIMO_ESPERA_MINUTOS} minutos: {faltantes}"
            )

        logger.info(
            "Aguardando atualização das fontes %s; nova verificação em %s segundos.",
            faltantes,
            INTERVALO_VERIFICACAO_SEGUNDOS,
        )
        sleep(INTERVALO_VERIFICACAO_SEGUNDOS)


@task(
    name="Materializar dev_atendimentos no BigQuery",
    retries=2,
    retry_delay_seconds=120,
)
def materializar_dev_atendimentos() -> str:
    """Executa o SQL versionado e aguarda a substituição da tabela."""
    logger = get_run_logger()
    sql_path = Path(__file__).with_name("dev_atendimentos.sql")
    sql = sql_path.read_text(encoding="utf-8")
    job = _cliente_bigquery().query(sql)
    job.result()
    logger.info(
        "Tabela %s substituída com sucesso (job_id=%s).",
        TABELA_DESTINO,
        job.job_id,
    )
    return job.job_id


@task(name="Validar materialização de dev_atendimentos")
def validar_dev_atendimentos() -> dict[str, str | int]:
    """Confirma que a tabela materializada tem linhas e uma data máxima."""
    logger = get_run_logger()
    linha = next(iter(_cliente_bigquery().query(CONSULTA_VALIDACAO).result()))
    if not linha.linhas or not linha.data_maxima:
        raise RuntimeError(
            f"A validação encontrou a tabela {TABELA_DESTINO} vazia "
            "ou sem data de atendimento."
        )

    resultado = {
        "linhas": int(linha.linhas),
        "data_maxima": linha.data_maxima.isoformat(),
    }
    logger.info(
        "Validação concluída: %s linhas; data máxima %s.",
        resultado["linhas"],
        resultado["data_maxima"],
    )
    return resultado
