-- Camada Raw: solicitações de vagas para acolhimento.
select
    seqsolic as id_solicitacao,
    seqpac as id_usuario,
    sequs as id_unidade,
    datsolic as data_solicitacao,
    horsolic as hora_solicitacao,
    indsolic as indicador_solicitacao,
    indstatsolic as status_solicitacao,
    datsolicatend as data_solicitacao_atendida,
    sequsatend as id_unidade_atendimento,
    sequssolic as id_unidade_solicitada,
    seqentsolic as id_entrada_solicitacao,
    _airbyte_extracted_at
from {{ source('brutos_acolherio_staging', 'gh_solic_vagas') }}
