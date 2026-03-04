
with cbo as (
    select
        cbo_sk,
        codcbo,
        seqprof,
        data_cadastro_cbo
    from {{ ref('int_cbo') }}
)

select * from cbo