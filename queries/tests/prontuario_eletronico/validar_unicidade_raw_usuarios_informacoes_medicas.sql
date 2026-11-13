select id_usuario
from {{ ref('raw_usuarios_informacoes_medicas') }}
group by id_usuario
having count(*) > 1
