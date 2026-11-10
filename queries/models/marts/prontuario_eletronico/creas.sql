{{ config(alias='dim_creas', schema='gerenciamento__dbt', enabled=target.name != 'prod', tags=['planilha_unificada', 'pu_dimensoes']) }}
select
    id_usuario,
    historico,
    participacoes
from {{ ref('int_perfil_creas') }}
