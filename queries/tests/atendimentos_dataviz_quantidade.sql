select u.id_usuario
from {{ ref('mart_atendimentos') }} as u
where
    u.quantidade_atendimentos is distinct from array_length(u.atendimentos)
    or u.quantidade_atendimentos < 1
    or u.atendimentos is null
