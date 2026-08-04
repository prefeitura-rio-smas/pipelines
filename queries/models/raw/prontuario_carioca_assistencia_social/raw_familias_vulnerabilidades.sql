-- Camada Raw: Vulnerabilidades das famílias
with source as (
    select 
        seqfamil as id_familia,
        seqlogincad as id_login_cadastro,
        seqvulnerab as id_vulnerabilidade,
        datcadastr as data_cadastro,
        datcancel as data_cancelamento
    from {{ source('prontuario_carioca_assistencia_social', 'gh_famil_vulnerab') }}
)
select * from source
