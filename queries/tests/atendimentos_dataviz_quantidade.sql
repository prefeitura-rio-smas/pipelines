select id_usuario
from {{ ref('mart_atendimentos') }}
where
    quantidade_atendimentos is distinct from array_length(atendimentos)
    or quantidade_atendimentos < 1
