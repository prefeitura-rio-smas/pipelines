-- Tabela responsável por retornar todas as famílias em descumprimento de condicionalidades do programa bolsa família;
-- Não foi retirada famílias de usuários testes;

with famil_vulnerabilidades as (
    select 
        seqfamil,
        datcadastr,
        seqlogincad,
        seqvulnerab
    from {{ source('cras_rma_prod', 'gh_famil_vulnerab') }}
    where datcancel is null
    and seqfamil = 1 -- Id do descumprimento de condicionalidades
)

select * from famil_vulnerabilidades