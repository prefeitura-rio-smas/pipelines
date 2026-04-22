-- Camada Raw: Projetos sociais das famílias
with source as (
    select
        seqfamil as id_familia,
        seqprojsoc as id_projeto_social,
        seqlogincad as id_login_cadastro,
        datcadastr as data_cadastro,
        datcancel as data_cancelamento
    from {{ source('brutos_acolherio_staging', 'gh_famil_projsociais') }}
)
select * from source
