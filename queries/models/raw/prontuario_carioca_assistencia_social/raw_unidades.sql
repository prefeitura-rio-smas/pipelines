-- Camada Raw: Limpeza e renomeação inicial da fonte de unidades
-- Sistema original: Prontuário Carioca de Assistência Social (ex-Acolherio)

with source as (
    select
        sequs as id_unidade,
        dscus as nome_unidade,
        apus as cas,
        siguf as uf,
        esfera,
        seqtipous as id_tipo_unidade,
        emailprof as email_unidade,
        (indinativo <> 'S') as flag_unidade_ativa
    from {{ source('brutos_acolherio_staging', 'gh_us') }}
)

select * from source
