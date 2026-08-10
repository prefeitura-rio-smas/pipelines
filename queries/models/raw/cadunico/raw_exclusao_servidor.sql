-- Camada Raw: exclusao_servidor
with source as (
    select
        id_prefeitura_exclusao as id_prefeitura,
        id_familia_exclusao as id_familia,
        cpf_operador_exclusao as cpf_operador,
        cpf_servidor_parecer_gestao_municipal_cadunico as cpf_servidor_parecer,
        data_emissao_parecer_gestao_municipal_cadunico as data_emissao_parecer,
        data_exclusao,
        descricao_cotivo_exclusao as descricao_exclusao,
        id_motivo_exclusao,
        motivo_exclusao,
        id_municipio_parecer_gestao_municipal_cadunico as id_municipio_parecer,
        servidor_parecer_gestao_municipal_cadunico as servidor_parecer,
        numero_parecer_gestao_municipal_cadunico as numero_parecer,
        numero_registro_arquivo as numero_registro,
        sigla_uf_parecer_gestao_municipal_cadunico as uf_parecer,
        versao_layout,
        data_particao
    from {{ source('cadunico', 'exclusao_servidor') }}
)

select * from source
