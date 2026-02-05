
with cbo as (
    select
        {{ dbt_utils.generate_surrogate_key(['codcbo', 'seqprof']) }} as cbo_sk,
        codcbo,
        seqprof,
        data_cadastro_cbo
    from {{ ref('stg_cbo') }}
)

select * from cbo