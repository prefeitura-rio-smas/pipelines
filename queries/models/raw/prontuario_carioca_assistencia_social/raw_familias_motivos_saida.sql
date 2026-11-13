select
    seqmotivsaid as id_motivo_saida,
    dscmotivsaid as descricao_motivo_saida,
    indativo as indicador_ativo
from {{ source('brutos_acolherio_staging', 'gh_familias_motiv_saida') }}
