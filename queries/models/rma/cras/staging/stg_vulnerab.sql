with vulnerabilidades as (
    select 
        seqfamil,
        datcadastr,
        extract(month from datcadastr) as mes_cadastro,
        extract(year from datcadastr) as ano_cadastro,
        seqlogincad,
        seqvulnerab,
        case
            when seqvulnerab = 1
            then 'Sim'
            else 'Não'
        end as flag_descumprimento_condicionalidade_bf
    from {{ source('cras_rma_prod', 'gh_famil_vulnerab') }}
    where datcancel is null
)

select 
    *
from vulnerabilidades
where seqvulnerab = 1

