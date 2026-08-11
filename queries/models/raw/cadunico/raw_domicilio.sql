-- Camada Raw: domicilio
with source as (
    select
        id_prefeitura,
        id_forma_abatecimento_agua_domicilio,
        forma_abatecimento_agua_domicilio,
        id_possui_agua_encanada_domicilio,
        possui_agua_encanada_domicilio,
        id_possui_banheiro_domicilio,
        possui_banheiro_domicilio,
        id_calcamento_domicilio,
        calcamento_domicilio,
        id_destino_lixo_domicilio,
        destino_lixo_domicilio,
        id_escoamento_sanitario_domicilio,
        escoamento_sanitario_domicilio,
        id_especie_domicilio,
        especie_domicilio,
        id_familia,
        id_iluminacao_domicilio,
        iluminacao_domicilio,
        id_local_domicilio,
        local_domicilio,
        id_material_domicilio,
        material_domicilio,
        id_material_piso_domicilio,
        material_piso_domicilio,
        numero_registro_arquivo,
        versao_layout,
        data_particao
    from {{ source('cadunico', 'domicilio') }}
)

select * from source
