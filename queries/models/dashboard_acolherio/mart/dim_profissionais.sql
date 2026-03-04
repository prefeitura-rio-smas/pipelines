-- Tabela dimensão para profissionais

with profissionais as (
    select 
        *
    from {{ ref('int_profissionais') }} 
)

select * from profissionais