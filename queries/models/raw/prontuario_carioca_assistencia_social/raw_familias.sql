-- Camada Raw: Cadastro base de famílias
with source as (
    select
        seqfamil as id_familia,
        seqpac_responsavel as id_usuario_responsavel,
        datultalteracao as data_ultima_modificacao,
        indativo as flag_ativo
    from {{ source('prontuario_carioca_assistencia_social', 'gh_familias') }}
)
select * from source
