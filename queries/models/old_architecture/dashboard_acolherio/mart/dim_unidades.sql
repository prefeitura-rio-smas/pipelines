-- Tabela dimensão de unidades

with unidades_tratadas as (
    select
        *
    from {{ ref('int_unidades') }}
)

select * from unidades_tratadas