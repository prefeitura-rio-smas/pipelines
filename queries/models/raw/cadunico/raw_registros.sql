-- Camada Raw: registros
with source as (
    select
        numero_registro_arquivo,
        versao_layout,
        data_particao
    from {{ source('cadunico', 'registros') }}
)

select * from source
