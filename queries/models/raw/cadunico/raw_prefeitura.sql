-- Camada Raw: prefeitura
with source as (
    select
        id_prefeitura,
        sigla_uf as id_uf,
        if_municipio as id_municipio,
        id_migracao,
        migracao,
        prefeitura as municipio,
        numero_registro_arquivo as numero_registro,
        id_versao_layout_arquivo,
        data_particao
    from {{ source('cadunico', 'prefeitura') }}
)

select * from source
