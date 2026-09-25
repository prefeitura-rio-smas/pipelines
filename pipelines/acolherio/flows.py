"""Flow Prefect para atualizar a fonte dev do Looker AcolheRio."""

from prefect import flow

from pipelines.acolherio.tasks import (
    aguardar_fontes_atualizadas,
    materializar_dev_atendimentos,
    validar_dev_atendimentos,
)


@flow(name="AcolheRio | Atualizar dev_atendimentos")
def atualizar_dev_atendimentos_flow() -> dict[str, str | int]:
    """Espera a carga Airbyte e atualiza a tabela consumida pelo Looker."""
    frescor = aguardar_fontes_atualizadas()
    job_id = materializar_dev_atendimentos()
    validacao = validar_dev_atendimentos()
    return {
        "fontes_atualizadas_utc": str(frescor),
        "job_id": job_id,
        **validacao,
    }
