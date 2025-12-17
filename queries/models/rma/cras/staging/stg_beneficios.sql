select 
    nome_usuario,
    id_usuario as seqpac,
    beneficio
from {{ source('cras_rma_dev', 'tipo_beneficio')}}
where not regexp_contains(nome_usuario, r'(?i)teste')
