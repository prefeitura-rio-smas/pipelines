-- Camada Raw: habitacao
with source as (
    select
        id_prefeitura,
        contrato_pro_habitacao,
        id_familia,
        natureza_pro_habitacao,
        programa_pro_habitacao,
        id_membro_familia as id_membro,
        numero_registro_arquivo as numero_registro,
        versao_layout,
        data_particao
    from {{ source('cadunico', 'habitacao') }}
)

select * from source
