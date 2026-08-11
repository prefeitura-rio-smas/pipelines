-- Camada Raw: exclusao_servidor
with source as (
    select
        id_prefeitura_exclusao,
        id_familia_exclusao,
        cpf_operador_exclusao,
        cpf_servidor_parecer_gestao_municipal_cadunico,
        data_emissao_parecer_gestao_municipal_cadunico,
        data_exclusao,
        descricao_cotivo_exclusao,
        id_motivo_exclusao,
        motivo_exclusao,
        id_municipio_parecer_gestao_municipal_cadunico,
        servidor_parecer_gestao_municipal_cadunico,
        numero_parecer_gestao_municipal_cadunico,
        numero_registro_arquivo,
        sigla_uf_parecer_gestao_municipal_cadunico,
        id_versao_layout_arquivo,
        data_particao
    from {{ source('cadunico', 'exclusao_servidor') }}
)

select * from source
