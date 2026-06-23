-- Camada Raw: Serviços assistenciais das famílias
with source as (
    select
        seqfamil as id_familia,
        seqservassist as id_servico_assistencial,
        seqlogincad as id_login_cadastro,
        sequs as id_unidade,
        datcadastr as data_cadastro,
        datcancel as data_cancelamento
    from {{ source('brutos_acolherio_staging', 'gh_famil_servassist') }}
)
select * from source
