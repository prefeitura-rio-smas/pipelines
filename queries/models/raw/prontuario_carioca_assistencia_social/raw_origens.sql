-- Camada Raw: Origens de cadastro do sistema
with source as (
    select
        codorigem as id_origem,
        dscoripcsm as descricao_origem,
        codoriraas as codigo_raas,
        dscoriraas as descricao_raas,
        indativo as flag_ativo
    from {{ source('prontuario_carioca_assistencia_social', 'gh_origens') }}
)
select * from source
