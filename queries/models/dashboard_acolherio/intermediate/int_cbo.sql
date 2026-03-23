
with cbo_joined as (
    select
        b.codcbo,
        a.seqprof,
        b.profissional
    from {{ ref('stg_gh_prof_ocup') }} a
    left join {{ ref('stg_cbo') }} b
        on a.codcbo = b.codcbo
),

cbo as (
    select
        {{ dbt_utils.generate_surrogate_key(['seqprof']) }} as seqprof_sk,
        codcbo,
        seqprof,
        profissional,
    from cbo_joined
)

select * from cbo