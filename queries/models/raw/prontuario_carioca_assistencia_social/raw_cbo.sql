-- Camada Raw: Cadastro de CBO (Classificação Brasileira de Ocupações)
with source as (
    select
        codcbo as codigo_cbo,
        dsccbo as descricao,
    from {{ source('brutos_acolherio_staging', 'gh_cbo') }}
)
select * from source
