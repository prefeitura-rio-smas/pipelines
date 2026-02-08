from prefect import flow
from pipelines.arcgis.tasks import load_arcgis_to_bigquery
from pipelines.tasks import run_dbt_models
from pipelines.arcgis.primeira_infancia_carioca.tasks import apply_arcgis_feedback

@flow(name="Primeira Infância Carioca | Carga Unificada")
def primeira_infancia_carioca_flow() -> None:
    """
    Fluxo unificado para o projeto Primeira Infância Carioca.
    Orquestra extração, tratamento dbt e feedback para o ArcGIS.
    """
    
    # 1. Extração RAW: Controle CAS (Tabela de Ratificação)
    load_arcgis_to_bigquery(
        job_name="controle_cas",
        item_id="6855307d763b49f6bfb1c5d83b069952",
    )

    # 2. Extração RAW: Primeira Infância Carioca (Survey Principal)
    load_arcgis_to_bigquery(
        job_name="primeira_infancia_carioca",
        item_id="ef6fe5c04520445f91be8a57c4adcd96",
        layer_idx=0
    )

    # 3. Transformação: Executa modelos dbt da pasta 'pic'
    # Isso inclui as tabelas enriquecidas e os deltas de feedback
    run_dbt_models(model_name="pic")

    # 4. Feedback: Envia deltas do BQ de volta para o ArcGIS
    
    # 4.1 Update para Primeira Infância (Survey)
    apply_arcgis_feedback(
        item_id="ef6fe5c04520445f91be8a57c4adcd96",
        delta_table="delta_feedback_pic",
        layer_idx=0
    )

    # 4.2 Update para Controle CAS (Table)
    apply_arcgis_feedback(
        item_id="6855307d763b49f6bfb1c5d83b069952",
        delta_table="delta_feedback_controle",
        # layer_idx=0  # Para Table, o resolver cuida se precisa de index ou não
    )

if __name__ == "__main__":
    primeira_infancia_carioca_flow()
