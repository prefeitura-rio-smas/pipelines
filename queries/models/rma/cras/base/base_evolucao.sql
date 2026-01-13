-- Tabela responsável por retornar dados de evolução.

with evolucao as (
    select
        dscevopac,
        dtevopac as data_evolucao,
        sequs,
        seqpac,
        seqlogin
    from {{ source('cras_rma_prod', 'gh_evoluadm') }}
    where indtpevopac = 'F'
)

select * from evolucao 