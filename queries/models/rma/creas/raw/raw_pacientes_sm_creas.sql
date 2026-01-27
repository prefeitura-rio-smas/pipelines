-- Tabela responsável por capturar os dados oriundos do formulário 'Histórico de Institucionalização' para poder recuperar dados para o item B4 do Creas RMA bloco I
-- Usuários testes não foram retirados

with base_pacientes as (
    select 
        seqpacsm as seqpac,
        dscinfadic as dados_institucional
    from {{ source('cras_rma_prod', 'gh_pacientes_sm') }}

)

select * from base_pacientes