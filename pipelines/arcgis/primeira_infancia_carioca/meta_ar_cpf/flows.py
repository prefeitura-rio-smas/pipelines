from prefect import flow
from prefect_dbt.cli.commands import trigger_dbt_cli_command
from pipelines.arcgis.tasks import load_arcgis_to_bigquery
from pipelines.arcgis.primeira_infancia_carioca.tasks import apply_arcgis_feedback
from pipelines.arcgis.primeira_infancia_carioca.meta_ar_cpf.tasks import (
    apply_arcgis_adds,
    apply_arcgis_status_sync,
)
import os

ITEM_ID = "66155d9b7ccf47d89af31a2bc8ddc5eb"

# --- Subflows ---


@flow(name="Meta AR CPF | Extract")
def flow_extract_meta_ar_cpf():
    """Baixa dados do layer pic_meta_ar_cpf do ArcGIS."""
    return load_arcgis_to_bigquery(
        job_name="meta_ar_cpf",
        item_id=ITEM_ID,
        layer_idx=0,
    )


@flow(name="Meta AR CPF | Transform (dbt)")
def flow_transform_meta_ar_cpf():
    """Executa modelos dbt do meta_ar_cpf."""
    dbt_target = os.getenv("MODE", "staging")
    return trigger_dbt_cli_command(
        command=f"dbt run --select meta_ar_cpf --target {dbt_target}",
        project_dir="queries",
        profiles_dir="queries",
    )


# --- Flow Maestro (manual) ---


@flow(name="Meta AR CPF | Pipeline")
def meta_ar_cpf_pipeline():
    """
    Fluxo Maestro para o Meta AR CPF.
    Execução manual / mensal. Etapas:
      1. Extract: ArcGIS → BQ (arcgis_raw.meta_ar_cpf_raw)
      2. Transform: dbt models (delta_feedback)
      3. Adds: novos ingressos para o ArcGIS (adds + crosswalk)
      4. Updates: colunas-fonte alteradas (delta VIEW → applyEdits updates)
      5. Status: marca egressos (status_sinc='egresso')
    """
    flow_extract_meta_ar_cpf()
    flow_transform_meta_ar_cpf()
    apply_arcgis_adds(item_id=ITEM_ID, layer_idx=0)
    apply_arcgis_feedback(
        item_id=ITEM_ID,
        delta_table="delta_feedback_meta_ar_cpf",
        layer_idx=0,
    )
    apply_arcgis_status_sync(item_id=ITEM_ID, layer_idx=0)


if __name__ == "__main__":
    meta_ar_cpf_pipeline()
