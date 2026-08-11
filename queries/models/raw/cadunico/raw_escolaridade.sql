-- Camada Raw: escolaridade
with source as (
    select
        id_prefeitura,
        id_ano_serie_frequenta,
        ano_serie_frequenta,
        id_ultimo_ano_serie_frequentou,
        ultimo_ano_serie_frequentou,
        id_inep_escola,
        id_concluiu_curso_frequentado,
        id_curso_frequenta,
        curso_frequenta,
        id_curso_mais_elevado_frequentou,
        curso_mais_elevado_frequentou,
        id_escola_localizada_municipio,
        id_familia,
        id_municipio_escola,
        id_origem_dados_escolaridade,
        origem_dados_escolaridade,
        id_sabe_ler_escrever,
        sabe_ler_escrever,
        data_integracao_escolaridade_membro,
        escola_nao_tem_inep,
        frequenta_escola,
        escola,
        municipio_escola,
        id_membro_familia,
        numero_registro_arquivo,
        sigla_uf_escola,
        versao_layout,
        data_particao
    from {{ source('cadunico', 'escolaridade') }}
)

select * from source
