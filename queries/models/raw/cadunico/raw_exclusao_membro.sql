-- Camada Raw: exclusao_membro
with source as (
    select
        id_prefeitura_membro_excluido as id_prefeitura,
        id_familia_membro_excluido as id_familia,
        folha_ceritao_obito_excluido as folha_certidao_obito,
        id_municipio_certidao_obito_excluido as id_municipio_certidao_obito,
        livro_certidao_obito_excluido as livro_certidao_obito,
        numero_termo_matricula_certidao_excluido as numero_termo_certidao,
        cpf_operador_exclusao_membro as cpf_operador_exclusao,
        cpf_servidor_parecer_gestao_municipal_cadunico_membro as cpf_servidor_parecer,
        data_emissao_parecer_gestao_municipal_cadunico_membro as data_emissao_parecer,
        data_exclusao_membro as data_exclusao,
        descricao_cotivo_exclusao as descricao_exclusao,
        data_emissao_certidao_obito_excluido as data_emissao_certidao_obito,
        id_motivo_exclusao_membro as id_motivo_exclusao,
        motivo_exclusao_membro as motivo_exclusao,
        id_municipio_parecer_gestao_municipal_cadunico_membro as id_municipio_parecer,
        cartorio_certidao_obito_excluido as cartorio_certidao_obito,
        municipio_certidao_obito_excluido as municipio_certidao_obito,
        servidor_parecer_gestao_municipal_cadunico_membro as servidor_parecer,
        id_membro_excluido as id_membro,
        numero_parecer_gestao_municipal_cadunico_membro as numero_parecer,
        numero_registro_arquivo as numero_registro,
        sigla_uf_certidao_obito_excluido as uf_certidao_obito,
        sigla_uf_parecer_gestao_municipal_cadunico_membro as uf_parecer,
        versao_layout,
        data_particao
    from {{ source('cadunico', 'exclusao_membro') }}
)

select * from source
