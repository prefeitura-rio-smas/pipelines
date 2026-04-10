-- Tabela intermediária do tipo de atendimento. Feita para criar a surrogate key.

with tipo_atendimentos as (
    select
        *,
        {{ dbt_utils.generate_surrogate_key(['seqtpatend']) }} as seqtpatend_sk
    from {{ ref('stg_tipo_atendimento') }}
)

select * from tipo_atendimentos