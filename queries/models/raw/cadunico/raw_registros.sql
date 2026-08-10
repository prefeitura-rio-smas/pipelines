-- Camada Raw: registros
with source as (
    select
        numero_registro_arquivo as numero_registro,
        versao_layout,
        data_particao
    from {{ source('cadunico', 'registros') }}
)

select * from source
