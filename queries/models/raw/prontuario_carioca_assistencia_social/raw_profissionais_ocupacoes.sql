-- Camada Raw: Vínculo entre profissionais e CBOs
with source as (
    select
        seqprof as id_profissional,
        codcbo as codigo_cbo
    from {{ source('brutos_acolherio_staging', 'gh_profocup') }}
)
select * from source
