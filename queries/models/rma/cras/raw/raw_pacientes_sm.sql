-- Tabela responsável por capturar os dados oriundos do formulário 'Histórico de Institucionalização' para poder recuperar dados para o item B6 do Cras RMA bloco I

with base_pacientes as (
    select 
        seqpacsm as seqpac,
        dscinfadic as dados_institucional
    from {{ source('cras_rma_prod', 'gh_pacientes_sm') }}

)

select * from base_pacientes