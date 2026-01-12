-- Tabela responsável por retornar informações relacionadas às contas dos operadores do Acolherio;
-- Contas testes, admin e suporte não foram retiradas

with informacao_contas as (
    select
        dsclogin as login_usuario,
        nompess as usuario,
        seqlogin
    from {{ source('cras_rma_prod', 'gh_contas')}} 
)

select * from informacao_contas
