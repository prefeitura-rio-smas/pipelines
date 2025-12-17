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


unir_tudo as (
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
) 

select * from unir_tudo