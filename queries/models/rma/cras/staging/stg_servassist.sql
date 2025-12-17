-- Tabela de famílias em acompanhamento paif
with famil_acomp_paif as (
    select
        seqfamil,
        datcadastr as data_original,
        extract(month from datcadastr) as mes_cadastro,
        extract(year from datcadastr) as ano_cadastro,
        seqlogincad,
        seqservassist
    from {{ source('cras_rma_prod', 'gh_famil_servassist')}}
    where datcancel is null
    and seqservassist = 1
),

tratar_paif as (
    select 
        seqfamil,
        data_original,
        mes_cadastro,
        ano_cadastro,
        seqlogincad,
        case 
            when seqservassist = 1 
            then 'Acompanhamento Paif'
        end as flag_acomp_paif
    from famil_acomp_paif

)

select * from tratar_paif