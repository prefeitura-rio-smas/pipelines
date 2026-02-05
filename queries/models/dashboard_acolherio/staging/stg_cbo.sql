-- Model para recuperar a ocupação de cada profissional

with profissional_cbo as (
    select
        codcbo,
        seqprof,
        dtcadast as data_cadastro_cbo
    from {{ source('brutos_acolherio_staging', 'gh_profocup') }}
)

select * from profissional_cbo
