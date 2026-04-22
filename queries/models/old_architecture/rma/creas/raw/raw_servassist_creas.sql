-- Tabela responsável por retornar todas as famílias em acompanhamento PAEFI;
-- Não foi retirada famílias de usuários testes;
{{ config(materialized = 'table') }}

with famil_acomp_paefi as (
    select
        seqfamil,
        datcadastr as data_cadastro_paefi,
        seqlogincad,
        seqservassist
    from {{ source('cras_rma_prod', 'gh_famil_servassist')}}
    where datcancel is null
    and seqservassist = 6 -- Id do acompanhamento PAIF
)

select * from famil_acomp_paefi