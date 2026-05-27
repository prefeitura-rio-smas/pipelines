-- Camada Raw: Tipos de unidade do sistema
with source as (
    select
        seqtipous as id_tipo_unidade,
        dsctipous as nome_tipo,
        indclasstu as classe,
        dscclassdetal as descricao_classe,
        dscclassresum as descricao_classe_resumida
    from {{ source('brutos_acolherio_staging', 'gh_us_tipo') }}
)
select * from source
