-- Tabela responsável por retornar todos os atendimentos de cada unidade

with atendimentos as (
    select
        unidade_atendimento as unidade,
        nome_atendimento,
        seq_atendimento
    from {{ source('cras_rma_dev', 'dev_atendimentos') }}
)

select * from atendimentos 