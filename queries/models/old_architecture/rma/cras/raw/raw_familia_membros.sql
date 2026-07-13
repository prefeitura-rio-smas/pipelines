-- Tabela responsável por retornar todas as famílias e seus respectivos membros;
-- Não foi retirada famílias de usuários testes;

with membros_familias as (
    select
        seqfamil,
        seqpac,
        seqmembro
    from {{ source('cras_rma_prod', 'gh_familias_membros') }}
    where datsaida is null
)

select * from membros_familias