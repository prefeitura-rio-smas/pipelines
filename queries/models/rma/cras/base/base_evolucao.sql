-- Tabela responsável por retornar dados de evolução.

with evolucao as (
    select
        dscevopac,
        dtevopac,
        sequs,
        seqpac,
        seqlogin
    from {{ source('cras_rma_prod', 'gh_evoluadm') }}
)

select * from evolucao 