with familias as (
    select
        seqfamil,
        seqpac,
        seqmembro
    from {{ source('cras_rma_prod', 'gh_familias_membros') }}
    where datsaida is null
),

membros_familia as (
    select
        a.seqfamil,
        a.seqmembro,
        b.indsexo,
        b.seqpac,
        b.datnascim as data_nascimento,
        b.dscnomepac as nome,
    from familias a
    inner join {{ source('cras_rma_prod', 'gh_cidadao_pac') }} b on a.seqpac = b.seqpac
)

select
    seqfamil,
    seqpac,
    seqmembro,
    indsexo,
    data_nascimento,
    nome,
    date_diff(current_date(), data_nascimento, year) as idade
from membros_familia
where not regexp_contains(nome, r'(?i)teste')