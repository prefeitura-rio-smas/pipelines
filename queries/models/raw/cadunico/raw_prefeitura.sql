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
        versao_layout,
        data_particao
    from {{ source('cadunico', 'prefeitura') }}
    where {{ filtro_particao_cadunico() }}
)

select * from source
