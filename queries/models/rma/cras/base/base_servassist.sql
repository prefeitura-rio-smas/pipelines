-- Tabela responsável por retornar todas as famílias em acompanhamento PAIF;
-- Não foi retirada famílias de usuários testes;

with famil_acomp_paif as (
    select
        seqfamil,
        datcadastr as data_original,
        seqlogincad,
        seqservassist
    from {{ source('cras_rma_prod', 'gh_famil_servassist')}}
    where datcancel is null
    and seqservassist = 1 -- Id do acompanhamento PAIF
)

select * from famil_acomp_paif