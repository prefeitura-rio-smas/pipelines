from prefect import flow
from prefect_dbt.cli.commands import trigger_dbt_cli_command
from pipelines.arcgis.tasks import load_arcgis_to_bigquery
from pipelines.arcgis.primeira_infancia_carioca.tasks import apply_arcgis_feedback
import os

# --- Subflows (Etapas Isoladas) ---

@flow(name="Extração | Controle CAS")
def flow_extract_controle_cas():
    """Baixa dados do Controle CAS (Original)."""
    return load_arcgis_to_bigquery(
        job_name="controle_cas",
        item_id="6855307d763b49f6bfb1c5d83b069952",
    )

@flow(name="Extração | Primeira Infância")
def flow_extract_primeira_infancia():
    """Baixa dados da Primeira Infância (Original)."""
    return load_arcgis_to_bigquery(
        job_name="primeira_infancia_carioca",
        item_id="ef6fe5c04520445f91be8a57c4adcd96",
        layer_idx=0
    )

@flow(name="Transformação | dbt (PIC)")
def flow_transform_dbt():
    """Executa os modelos dbt do projeto PIC usando a integração nativa."""
    dbt_target = os.getenv("MODE", "staging")
    return trigger_dbt_cli_command(
        command=f"dbt run --select pic --target {dbt_target}",
        project_dir="queries",
        profiles_dir="queries"
    )

@flow(name="Feedback | Write-back ArcGIS")
def flow_feedback_arcgis():
    """Envia atualizações de volta para os itens originais do ArcGIS."""
    
    # 4.1 Update para Primeira Infância (Survey)
    feedback_pic = apply_arcgis_feedback(
        item_id="ef6fe5c04520445f91be8a57c4adcd96",
        delta_table="delta_feedback_pic",
        layer_idx=0
    )

    # 4.2 Update para Controle CAS (Table)
    feedback_controle = apply_arcgis_feedback(
        item_id="6855307d763b49f6bfb1c5d83b069952",
        delta_table="delta_feedback_controle",
    )
    return {"pic_updated": feedback_pic, "controle_updated": feedback_controle}

# --- Flow Maestro ---

@flow(name="Primeira Infância Carioca | Maestro")
def primeira_infancia_carioca_flow() -> None:
    """
    Fluxo Maestro para o projeto Primeira Infância Carioca.
    """
    flow_extract_controle_cas()
    flow_extract_primeira_infancia()
    flow_transform_dbt()
    flow_feedback_arcgis()

if __name__ == "__main__":
    primeira_infancia_carioca_flow()
