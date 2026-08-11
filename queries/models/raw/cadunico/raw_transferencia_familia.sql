-- Camada Raw: transferencia_familia
with source as (
    select
        id_prefeitura_antes_transferencia,
        id_familia_destino,
        id_prefeitura_destino,
        id_estado_cadastro_transferencia,
        estado_cadastro_transferencia,
        id_familia_antes_transferencia,
        id_uf_transferencia,
        id_municipio_transferencia,
        data_transferencia,
        numero_registro_transferencia,
        id_versao_layout_arquivo,
        data_particao
    from {{ source('cadunico', 'transferencia_familia') }}
)

select * from source
