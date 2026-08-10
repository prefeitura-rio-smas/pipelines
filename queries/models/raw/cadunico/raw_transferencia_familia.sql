-- Camada Raw: transferencia_familia
with source as (
    select
        id_prefeitura_antes_transferencia as id_prefeitura_origem,
        id_familia_destino,
        id_prefeitura_destino,
        id_estado_cadastro_transferencia,
        estado_cadastro_transferencia,
        id_familia_antes_transferencia as id_familia_origem,
        id_uf_transferencia as id_uf_origem,
        id_municipio_transferencia as id_municipio_origem,
        data_transferencia,
        numero_registro_transferencia,
        versao_layout,
        data_particao
    from {{ source('cadunico', 'transferencia_familia') }}
)

select * from source
