-- Camada Raw: exclusao_membro
with source as (
    select
        id_prefeitura_membro_excluido,
        id_familia_membro_excluido,
        folha_ceritao_obito_excluido,
        id_municipio_certidao_obito_excluido,
        livro_certidao_obito_excluido,
        numero_termo_matricula_certidao_excluido,
        cpf_operador_exclusao_membro,
        cpf_servidor_parecer_gestao_municipal_cadunico_membro,
        data_emissao_parecer_gestao_municipal_cadunico_membro,
        data_exclusao_membro,
        descricao_cotivo_exclusao,
        data_emissao_certidao_obito_excluido,
        id_motivo_exclusao_membro,
        motivo_exclusao_membro,
        id_municipio_parecer_gestao_municipal_cadunico_membro,
        cartorio_certidao_obito_excluido,
        municipio_certidao_obito_excluido,
        servidor_parecer_gestao_municipal_cadunico_membro,
        id_membro_excluido,
        numero_parecer_gestao_municipal_cadunico_membro,
        numero_registro_arquivo,
        sigla_uf_certidao_obito_excluido,
        sigla_uf_parecer_gestao_municipal_cadunico_membro,
        versao_layout,
        data_particao
    from {{ source('cadunico', 'exclusao_membro') }}
    where {{ filtro_particao_cadunico() }}
)

select * from source
