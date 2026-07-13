-- Tabela dimensão para profissionais
{{ config(materialized='ephemeral') }}

with profissionais as (
    select 
        *
    from {{ ref('int_profissionais') }} 
)

select * from profissionais