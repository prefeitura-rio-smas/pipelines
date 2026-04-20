-- Tabela int de unidades
-- Surrogate Key é gerada aqui

with gerar_surrogate_key as (
    select
        {{ dbt_utils.generate_surrogate_key(['sequs', 'unidade']) }} as sequs_sk,
        *
    from {{ ref('stg_unidades') }}
)

select * from gerar_surrogate_key