-- Camada Raw: notificações relacionadas a acolhimentos/regulações.
select
    seqnotif as id_notificacao,
    seqpac as id_usuario,
    sequs as id_unidade,
    seqregul as id_regulacao,
    datnotif as data_notificacao,
    indstatus as status_notificacao,
    _airbyte_extracted_at
from {{ source('brutos_acolherio_staging', 'gh_notif_acolhe') }}
