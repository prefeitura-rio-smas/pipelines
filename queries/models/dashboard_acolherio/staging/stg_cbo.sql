-- Model para recuperar a ocupação de cada profissional

with profissional_cbo as (
    select
        codcbo,
        dsccbo as descricao_funcao_prof,
    from {{ source('brutos_acolherio_staging', 'gh_cbo') }}
)

select * from profissional_cbo
