-- Camada Raw: Tipos de unidade do sistema
with source as (
    select
        seqtipous as id_tipo_unidade,
        dsctipous as nome_tipo,
        indclasstu as classe,
        dscclassdetal as descricao_classe,
        dscclassresum as descricao_classe_resumida
    from {{ source('prontuario_carioca_assistencia_social', 'gh_us_tipo') }}
)
select * from source
