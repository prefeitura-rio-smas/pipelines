-- Camada Raw: contato
with source as (
    select
        id_prefeitura,
        id_familia,
        cpf_operador_responsavel,
        data_arquivos_carregados,
        email,
        arquivos_carregados,
        autoriza_envio_email,
        contato_envio_sms as envio_sms,
        contato_2_envio_sms as envio_sms_2,
        id_contato_tipo,
        contato_tipo,
        id_contato_2_tipo,
        contato_2_tipo,
        id_email_tipo,
        email_tipo,
        contato_ddd as ddd,
        contato_2_ddd as ddd_2,
        numero_registro_arquivo as numero_registro,
        contato_telefone as telefone,
        contato_2_telefone as telefone_2,
        id_versao_layout_arquivo,
        data_particao
    from {{ source('cadunico', 'contato') }}
)

select * from source
