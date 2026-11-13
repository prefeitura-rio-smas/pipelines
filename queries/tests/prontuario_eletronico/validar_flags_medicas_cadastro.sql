select id_usuario
from {{ ref('raw_usuarios_informacoes_medicas') }}
where
    coalesce(upper(trim(cast(flag_uso_substancias_psicoativas as string))), '') not in ('', 'S', 'N')
    or coalesce(upper(trim(cast(flag_possui_comorbidade as string))), '') not in ('', 'S', 'N')
