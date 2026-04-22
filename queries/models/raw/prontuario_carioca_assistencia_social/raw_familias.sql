-- Camada Raw: Cadastro base de famílias
with source as (
    select
        seqfamil as id_familia,
        sequs as id_unidade,
        seqlogin as id_login_cadastro,
        datcadast as data_cadastro,
        datultmodif as data_ultima_modificacao,
        indativo as flag_ativo
    from {{ source('brutos_acolherio_staging', 'gh_familias') }}
)
select * from source
