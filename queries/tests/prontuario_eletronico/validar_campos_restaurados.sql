select usuario.id_usuario
from {{ ref('prontuario_eletronico') }}
where
    centro_pop.flag_empregabilidade_imediata is distinct from 'Não Informado'
    or centro_pop.capacidade_habilidades is distinct from 'Não Informado'
    or centro_pop.flag_interesse_curso is distinct from 'Não Informado'
