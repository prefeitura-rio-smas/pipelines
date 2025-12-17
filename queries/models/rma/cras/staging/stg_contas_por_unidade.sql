with prof_mais_de_uma_unidade as (
    select
        seqlogin,
        sequs,
        datacesso
    from {{ ref('stg_profissional_mais_de_uma_unidade')}}
),

prof_unidade as (
    select
        seqlogin,
        sequs,
        datacesso
    from {{ source('cras_rma_prod','gh_contas_us')}}
    where datacesso is null
    and seqlogin not in (select seqlogin from prof_mais_de_uma_unidade)
),

total_prof_unidade as (
    select
        seqlogin,
        sequs,
        datacesso
    from prof_unidade
    union distinct
    select
        seqlogin,
        sequs,
        datacesso
    from prof_mais_de_uma_unidade
),

nome_conta as (
    select
      a.seqlogin,
      a.sequs,
      a.datacesso,
      b.nompess
    from total_prof_unidade a
    inner join {{ source('cras_rma_prod', 'gh_contas') }} b on a.seqlogin = b.seqlogin
)

select
    *
from nome_conta
where not regexp_contains(nompess, r'(?i)teste|admin|suporte')



