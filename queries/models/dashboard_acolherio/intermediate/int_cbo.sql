
with cbo_joined as (
    select
        b.codcbo,
        a.seqprof,
        b.descricao_funcao_prof
    from {{ ref('stg_gh_prof_ocup') }} a
    left join {{ ref('stg_cbo') }} b
        on a.codcbo = b.codcbo
),

cbo as (
    select
        {{ dbt_utils.generate_surrogate_key(['seqprof']) }} as seqprof_sk,
        codcbo,
        seqprof,
        descricao_funcao_prof,
        row_number() over (
            partition by
                seqprof
        ) as rn 
    from cbo_joined
)

-- Normalizando a tabela de CBO para apenas 1 cardinalidade.

select * from cbo
where rn = 1