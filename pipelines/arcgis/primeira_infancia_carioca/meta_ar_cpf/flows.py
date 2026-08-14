import os

from prefect import flow
from prefect_dbt.cli.commands import trigger_dbt_cli_command

from pipelines.arcgis.primeira_infancia_carioca.meta_ar_cpf.tasks import (
    DATASET,
    apply_arcgis_adds,
    apply_arcgis_status_sync,
)
from pipelines.arcgis.primeira_infancia_carioca.tasks import apply_arcgis_feedback
from pipelines.arcgis.tasks import load_arcgis_to_bigquery

ITEM_ID = "66155d9b7ccf47d89af31a2bc8ddc5eb"

DELTA_TABLE = (
    "delta_feedback_meta_acordo_resultados_cpf"
    if DATASET == "pequenos_cariocas"
    else "delta_feedback_meta_ar_cpf"
)

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
        command=f"dbt run --select tag:meta_ar_cpf --target {dbt_target}",
        project_dir="queries",
        profiles_dir="queries",
    )


# --- Subflow: Write-back ---


@flow(name="Meta AR CPF | Write-back")
def flow_feedback_meta_ar_cpf():
    """Envia atualizações de volta para o ArcGIS."""
    adds = apply_arcgis_adds(item_id=ITEM_ID, layer_idx=0)

    updates = apply_arcgis_feedback(
        item_id=ITEM_ID,
        delta_table=DELTA_TABLE,
        layer_idx=0,
        dataset=DATASET,
    )

    sync = apply_arcgis_status_sync(item_id=ITEM_ID, layer_idx=0)
    return {"adds": adds, "updates": updates, "sync": sync}


# --- Flows Públicos ---


@flow(name="Meta AR CPF | Operador Sync")
def meta_ar_cpf_operador_sync():
    """
    Sincroniza dados do operador do ArcGIS para o BQ (raw).
    Horário em prod (1x/h). Em staging: manual.
    """
    return flow_extract_meta_ar_cpf()


@flow(name="Meta AR CPF | Cadúnico Sync")
def meta_ar_cpf_cadunico_sync():
    """
    Atualiza dados do CadÚnico e sincroniza com ArcGIS.
    Execução manual / mensal.
    Etapas:
      1. Extract: ArcGIS → BQ (arcgis_raw.meta_ar_cpf_raw)
      2. Transform: dbt models (delta_feedback)
      3. Write-back: adds, updates e sync no ArcGIS
    """
    flow_extract_meta_ar_cpf()
    flow_transform_meta_ar_cpf()
    flow_feedback_meta_ar_cpf()


if __name__ == "__main__":
    print("Use 'prefect deployment run ...' para executar os flows.")
    print("  Meta AR CPF | Operador Sync  (extract only, horário)")
    print("  Meta AR CPF | Cadúnico Sync  (ciclo completo, manual)")
