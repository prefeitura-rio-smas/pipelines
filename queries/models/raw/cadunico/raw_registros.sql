-- Camada Raw: registros
with source as (
    select
        numero_registro_arquivo,
        id_versao_layout_arquivo,
        data_particao
    from {{ source('cadunico', 'registros') }}
)

select * from source
