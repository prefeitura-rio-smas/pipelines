-- Camada Raw: Vínculo entre profissionais e CBOs
with source as (
    select
        seqprof as id_profissional,
        codcbo as codigo_cbo
    from {{ source('prontuario_carioca_assistencia_social', 'gh_profocup') }}
)
select * from source
