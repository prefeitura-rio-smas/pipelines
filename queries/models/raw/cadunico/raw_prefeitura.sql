-- Camada Raw: prefeitura
with source as (
    select
        id_prefeitura,
        sigla_uf,
        if_municipio,
        id_migracao,
        migracao,
        prefeitura,
        numero_registro_arquivo,
        id_versao_layout_arquivo,
        data_particao
    from {{ source('cadunico', 'prefeitura') }}
)

select * from source
