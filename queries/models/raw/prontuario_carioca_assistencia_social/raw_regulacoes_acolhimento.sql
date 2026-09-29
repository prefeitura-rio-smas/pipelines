-- Camada Raw: regulações de acolhimento.
select
    seqregul as id_regulacao,
    seqpac as id_usuario,
    sequs as id_unidade,
    datregul as data_regulacao,
    datentr as data_entrada_prevista,
    datcancel as data_cancelamento,
    indvagasolcomter as indicador_vaga_com_terceiro,
    _airbyte_extracted_at
from {{ source('brutos_acolherio_staging', 'gh_pac_regul') }}
