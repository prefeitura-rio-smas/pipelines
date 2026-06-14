{{ config(materialized='table') }}

with source as (
    select * from {{ ref('int_profissionais_unidades') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['id_login', 'id_unidade', "coalesce(cbo_codigo, 'SEM_CBO')"]) }} as id_profissional_unidade_sk,
    *
from source
