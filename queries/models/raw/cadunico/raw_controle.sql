-- Camada Raw: controle
with source as (
    select
        id_versao_layout_arquivo,
        data_extracao_dados,
        data_posicao_cadastro,
        nome_arquivo,
        numero_registro_arquivo as numero_registro,
        versao_layout,
        data_particao
    from {{ source('cadunico', 'controle') }}
)

select * from source
