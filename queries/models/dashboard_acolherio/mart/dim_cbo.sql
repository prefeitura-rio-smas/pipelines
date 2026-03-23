
with cbo as (
    select
        seqprof_sk,
        codcbo,
        profissional,
        seqprof
    from {{ ref('int_cbo') }}
)

select * from cbo