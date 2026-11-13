select id_paciente
from {{ ref('raw_usuarios_detalhes') }}
group by id_paciente
having count(*) > 1
