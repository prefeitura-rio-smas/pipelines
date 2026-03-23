-- Model para recuperar a ocupação de cada profissional

with profissional_cbo as (
    select
        codcbo,
        dsccbo as profissional,
    from {{ source('brutos_acolherio_staging', 'gh_cbo') }}
)

select * from profissional_cbo
