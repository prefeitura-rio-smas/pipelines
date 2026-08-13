-- Camada Raw: identificacao_membro
with source as (
    select
        id_prefeitura_origem,
        id_certidao_registrada_cartorio,
        certidao_registrada_cartorio,
        id_familia_destino_transferencia,
        id_prefeitura_destino_transferencia,
        id_estado_cadastro_transferencia_membro,
        estado_cadastro_transferencia_membro,
        id_familia_origem,
        id_municipio_nascimento,
        id_pais_nascimento,
        id_raca_cor,
        raca_cor,
        id_sexo,
        sexo,
        data_nascimento,
        data_transferencia_membro,
        filiacao_1_nome_completo_mae_membt,
        filiacao_2,
        id_filiacao_1_nom_completo_mae_membt,
        filiacao_1_nom_completo_mae_membt,
        id_filiacao_2,
        sabe_municipio_nascimento,
        nao_sabe_nome_mae,
        nao_sabe_nome_pai,
        nao_sabe_pais_nascimento,
        sabe_sigla_uf_nascimento,
        apelido,
        nome_mae,
        nome_pai,
        municipio_nascimento,
        nome,
        pais_nascimento,
        id_membro_familia,
        nis,
        numero_registro_arquivo,
        sigla_uf_nascimento,
        versao_layout,
        data_particao
    from {{ source('cadunico', 'identificacao_membro') }}
    where {{ filtro_particao_cadunico() }}
)

select * from source
