-- Camada Raw: Cadastro de CBO (Classificação Brasileira de Ocupações)
with source as (
    select
        codcbo as codigo_cbo,
        dsccbo as descricao,
    from {{ source('prontuario_carioca_assistencia_social', 'gh_cbo') }}
)
select * from source
