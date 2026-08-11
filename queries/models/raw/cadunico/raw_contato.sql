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
        contato_envio_sms,
        contato_2_envio_sms,
        id_contato_tipo,
        contato_tipo,
        id_contato_2_tipo,
        contato_2_tipo,
        id_email_tipo,
        email_tipo,
        contato_ddd,
        contato_2_ddd,
        numero_registro_arquivo,
        contato_telefone,
        contato_2_telefone,
        versao_layout,
        data_particao
    from {{ source('cadunico', 'contato') }}
    where {{ filtro_particao_cadunico() }}
)

select * from source
