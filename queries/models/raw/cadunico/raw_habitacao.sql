-- Camada Raw: habitacao
with source as (
    select
        id_prefeitura,
        contrato_pro_habitacao,
        id_familia,
        natureza_pro_habitacao,
        programa_pro_habitacao,
        id_membro_familia,
        numero_registro_arquivo,
        versao_layout,
        data_particao
    from {{ source('cadunico', 'habitacao') }}
)

select * from source
