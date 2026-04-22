
with cbo as (
    select
        seqprof_sk,
        codcbo,
        descricao_funcao_prof,
        seqprof
    from {{ ref('int_cbo') }}
)

select * from cbo