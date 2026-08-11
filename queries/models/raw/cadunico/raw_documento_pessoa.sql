-- Camada Raw: documento_pessoa
with source as (
    select
        id_prefeitura,
        id_cartorio_certidao,
        id_certidao_civil,
        certidao_civil,
        id_complemento_rg,
        id_familia,
        folha_certidao,
        id_municipio_certidao,
        livro_certidao_obito_excluido,
        id_termi_matricula_certidao,
        data_emissao_carteira_trabalho,
        data_emissao_certidao,
        data_emissao_rg,
        cartorio_certidao,
        municipio_certidao,
        id_carteira_trabalho,
        cpf,
        rg,
        id_membro_familia,
        numero_registro_arquivo,
        id_secao_titulo_eleitor,
        id_serie_carteira_trabalho,
        id_titulo_eleitor,
        id_zona_titulo_eleitor,
        orgao_emissor_rg,
        sigla_uf_carteira_trabalho,
        sigla_uf_certidao,
        sigla_uf_rg,
        id_versao_layout_arquivo,
        data_particao
    from {{ source('cadunico', 'documento_pessoa') }}
)

select * from source
