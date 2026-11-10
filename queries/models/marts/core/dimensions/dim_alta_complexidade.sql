{{ config(enabled=target.name != 'prod', tags=['planilha_unificada', 'pu_dimensoes']) }}
select
    id_usuario,
    historico
from {{ ref('int_perfil_alta_complexidade') }}
