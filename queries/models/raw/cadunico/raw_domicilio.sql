-- Camada Raw: domicilio
with source as (
    select
        id_prefeitura,
        id_forma_abatecimento_agua_domicilio as id_forma_abastecimento_agua,
        forma_abatecimento_agua_domicilio as forma_abastecimento_agua,
        id_possui_agua_encanada_domicilio as id_possui_agua_encanada,
        possui_agua_encanada_domicilio as possui_agua_encanada,
        id_possui_banheiro_domicilio as id_possui_banheiro,
        possui_banheiro_domicilio as possui_banheiro,
        id_calcamento_domicilio as id_calcamento,
        calcamento_domicilio as calcamento,
        id_destino_lixo_domicilio as id_destino_lixo,
        destino_lixo_domicilio as destino_lixo,
        id_escoamento_sanitario_domicilio as id_escoamento_sanitario,
        escoamento_sanitario_domicilio as escoamento_sanitario,
        id_especie_domicilio as id_especie,
        especie_domicilio as especie,
        id_familia,
        id_iluminacao_domicilio as id_iluminacao,
        iluminacao_domicilio as iluminacao,
        id_local_domicilio as id_local,
        local_domicilio as local,
        id_material_domicilio as id_material,
        material_domicilio as material,
        id_material_piso_domicilio as id_material_piso,
        material_piso_domicilio as material_piso,
        numero_registro_arquivo as numero_registro,
        id_versao_layout_arquivo,
        data_particao
    from {{ source('cadunico', 'domicilio') }}
)

select * from source
