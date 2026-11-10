{{ config(enabled=target.name != 'prod', tags=['planilha_unificada', 'pu_dimensoes']) }}
select * from {{ ref('int_perfil_alta_complexidade') }}
