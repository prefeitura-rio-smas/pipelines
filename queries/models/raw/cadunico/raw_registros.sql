-- Camada Raw: registros
with source as (
    select
        numero_registro_arquivo,
        registros_tipo_00,
        registros_tipo_01,
        registros_tipo_02,
        registros_tipo_03,
        registros_tipo_04,
        registros_tipo_05,
        registros_tipo_06,
        registros_tipo_07,
        registros_tipo_08,
        registros_tipo_09,
        registros_tipo_10,
        registros_tipo_11,
        registros_tipo_12,
        registros_tipo_13,
        registros_tipo_14,
        registros_tipo_15,
        registros_tipo_16,
        registros_tipo_17,
        registros_tipo_18,
        registros_tipo_19,
        registros_tipo_20,
        registros_tipo_21,
        registros_tipo_98,
        registros_tipo_99,
        versao_layout,
        data_particao
    from {{ source('cadunico', 'registros') }}
    where {{ filtro_particao_cadunico() }}
)

select * from source
