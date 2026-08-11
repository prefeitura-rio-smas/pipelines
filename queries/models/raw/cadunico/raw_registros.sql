-- Camada Raw: registros
with source as (
    select
        numero_registro_arquivo,
        versao_layout,
        data_particao
    from {{ source('cadunico', 'registros') }}
    where {{ filtro_particao_cadunico() }}
)

select * from source
