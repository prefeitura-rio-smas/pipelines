-- Camada Raw: Evoluções do módulo Família
with source as (
    select
        seqfamil as id_familia,
        sequs as id_unidade,
        seqlogin as id_login,
        dtevofamil as data_evolucao,
        dscevofamil as descricao_evolucao,
        indtpevofamil as tipo_evolucao
    from {{ source('brutos_acolherio_staging', 'gh_evolufamil') }}
)
select * from source
