-- models/old_architecture/pic/meta_ar_cpf/delta_feedback_meta_ar_cpf.sql
-- Registros da _dev que precisam ser atualizados no ArcGIS pic_meta_ar_cpf
-- Comparação campo a campo entre o estado atual (BQ) e a última extração (raw ArcGIS).

{{ config(materialized='view', tags=['meta_ar_cpf']) }}

WITH
    calculado AS (
        SELECT
            cw.objectid_arcgis AS objectid,
            dev.cpf_pic, dev.cpf_cadun, dev.id_familia, dev.id_membro_familia,
            dev.nome, dev.sexo,
            CAST(dev.nascimento_data AS STRING) AS nascimento_data,
            CAST(dev.idade AS STRING) AS idade,
            dev.subprefeitura, dev.regiao_administrativa, dev.bairro,
            dev.grupo, dev.grupo_detalhado, dev.status, dev.cas,
            dev.protocolo_secretaria, dev.protocolo_id, dev.protocolo_descricao,
            dev.protocolo_status,
            CAST(dev.protocolo_data_referencia AS STRING) AS protocolo_data_referencia,
            dev.condicao_cadastro_cadun,
            CAST(dev.data_atualizacao_cadun AS STRING) AS data_atualizacao_cadun,
            dev.nome_rf_cadun, dev.cpf_rf_cadun, dev.telefone_cadun,
            dev.bairro_cadun, dev.unidade_territorial_cadun, dev.endereco_cadun,
            dev.complemento_cadun, dev.complemento_adicional_cadun,
            dev.refencia_logradouro_cadun,
            CAST(dev.data_particao_cadun AS STRING) AS data_particao_cadun,
            CAST(dev.data_entrada AS STRING) AS data_entrada,
            CAST(dev.data_saida AS STRING) AS data_saida,
            CAST(dev.flag_ativo AS STRING) AS flag_ativo,
            CAST(dev.flag_atual AS STRING) AS flag_atual,
            CAST(dev.numero_entrada AS STRING) AS numero_entrada,
            'ativo' AS status_sinc
        FROM rj-smas-dev.pic.crosswalk_meta_ar_cpf cw
        JOIN rj-smas-dev.pic.pequenos_cariocas_meta_ar_cpf_dev dev
          ON cw.id_membro_familia = dev.id_membro_familia
        WHERE dev.flag_atual = true
    ),

    atual AS (
        SELECT *
        FROM {{ source('arcgis_raw', 'meta_ar_cpf_raw') }}
    )

SELECT
    calculado.*
FROM calculado
JOIN atual ON CAST(calculado.objectid AS STRING) = atual.objectid
WHERE
    -- Campo a campo. O padrão NULLIF(..., 'None') trata o value 'None' que
    -- o load_arcgis_to_bigquery grava quando o valor original é nulo.
    COALESCE(NULLIF(calculado.cpf_pic, 'None'), '')                  != COALESCE(NULLIF(atual.cpf_pic, 'None'), '')
    OR COALESCE(NULLIF(calculado.cpf_cadun, 'None'), '')             != COALESCE(NULLIF(atual.cpf_cadun, 'None'), '')
    OR COALESCE(NULLIF(calculado.nome, 'None'), '')                  != COALESCE(NULLIF(atual.nome, 'None'), '')
    OR COALESCE(NULLIF(calculado.sexo, 'None'), '')                  != COALESCE(NULLIF(atual.sexo, 'None'), '')
    OR COALESCE(NULLIF(CAST(calculado.nascimento_data AS STRING), 'None'), '') != COALESCE(NULLIF(atual.nascimento_data, 'None'), '')
    OR COALESCE(NULLIF(CAST(calculado.idade AS STRING), 'None'), '')    != COALESCE(NULLIF(CAST(atual.idade AS STRING), 'None'), '')
    OR COALESCE(NULLIF(calculado.subprefeitura, 'None'), '')         != COALESCE(NULLIF(atual.subprefeitura, 'None'), '')
    OR COALESCE(NULLIF(calculado.regiao_administrativa, 'None'), '') != COALESCE(NULLIF(atual.regiao_administrativa, 'None'), '')
    OR COALESCE(NULLIF(calculado.bairro, 'None'), '')                != COALESCE(NULLIF(atual.bairro, 'None'), '')
    OR COALESCE(NULLIF(calculado.grupo, 'None'), '')                 != COALESCE(NULLIF(atual.grupo, 'None'), '')
    OR COALESCE(NULLIF(calculado.grupo_detalhado, 'None'), '')       != COALESCE(NULLIF(atual.grupo_detalhado, 'None'), '')
    OR COALESCE(NULLIF(calculado.status, 'None'), '')                != COALESCE(NULLIF(atual.status, 'None'), '')
    OR COALESCE(NULLIF(calculado.cas, 'None'), '')                   != COALESCE(NULLIF(atual.cas, 'None'), '')
    OR COALESCE(NULLIF(calculado.protocolo_secretaria, 'None'), '')  != COALESCE(NULLIF(atual.protocolo_secretaria, 'None'), '')
    OR COALESCE(NULLIF(calculado.protocolo_id, 'None'), '')          != COALESCE(NULLIF(atual.protocolo_id, 'None'), '')
    OR COALESCE(NULLIF(calculado.protocolo_descricao, 'None'), '')   != COALESCE(NULLIF(atual.protocolo_descricao, 'None'), '')
    OR COALESCE(NULLIF(calculado.protocolo_status, 'None'), '')      != COALESCE(NULLIF(atual.protocolo_status, 'None'), '')
    OR COALESCE(NULLIF(CAST(calculado.protocolo_data_referencia AS STRING), 'None'), '') != COALESCE(NULLIF(atual.protocolo_data_referencia, 'None'), '')
    OR COALESCE(NULLIF(calculado.condicao_cadastro_cadun, 'None'), '') != COALESCE(NULLIF(atual.condicao_cadastro_cadun, 'None'), '')
    OR COALESCE(NULLIF(CAST(calculado.data_atualizacao_cadun AS STRING), 'None'), '') != COALESCE(NULLIF(atual.data_atualizacao_cadun, 'None'), '')
    OR COALESCE(NULLIF(calculado.nome_rf_cadun, 'None'), '')         != COALESCE(NULLIF(atual.nome_rf_cadun, 'None'), '')
    OR COALESCE(NULLIF(calculado.cpf_rf_cadun, 'None'), '')          != COALESCE(NULLIF(atual.cpf_rf_cadun, 'None'), '')
    OR COALESCE(NULLIF(calculado.telefone_cadun, 'None'), '')        != COALESCE(NULLIF(atual.telefone_cadun, 'None'), '')
    OR COALESCE(NULLIF(calculado.bairro_cadun, 'None'), '')          != COALESCE(NULLIF(atual.bairro_cadun, 'None'), '')
    OR COALESCE(NULLIF(calculado.unidade_territorial_cadun, 'None'), '') != COALESCE(NULLIF(atual.unidade_territorial_cadun, 'None'), '')
    OR COALESCE(NULLIF(calculado.endereco_cadun, 'None'), '')        != COALESCE(NULLIF(atual.endereco_cadun, 'None'), '')
    OR COALESCE(NULLIF(calculado.complemento_cadun, 'None'), '')     != COALESCE(NULLIF(atual.complemento_cadun, 'None'), '')
    OR COALESCE(NULLIF(calculado.complemento_adicional_cadun, 'None'), '') != COALESCE(NULLIF(atual.complemento_adicional_cadun, 'None'), '')
    OR COALESCE(NULLIF(calculado.refencia_logradouro_cadun, 'None'), '') != COALESCE(NULLIF(atual.refencia_logradouro_cadun, 'None'), '')
    OR COALESCE(NULLIF(CAST(calculado.data_particao_cadun AS STRING), 'None'), '') != COALESCE(NULLIF(atual.data_particao_cadun, 'None'), '')
    OR COALESCE(NULLIF(CAST(calculado.data_entrada AS STRING), 'None'), '') != COALESCE(NULLIF(atual.data_entrada, 'None'), '')
    OR COALESCE(NULLIF(CAST(calculado.data_saida AS STRING), 'None'), '') != COALESCE(NULLIF(atual.data_saida, 'None'), '')
    OR COALESCE(NULLIF(CAST(calculado.flag_ativo AS STRING), 'None'), '') != COALESCE(NULLIF(atual.flag_ativo, 'None'), '')
    OR COALESCE(NULLIF(CAST(calculado.flag_atual AS STRING), 'None'), '') != COALESCE(NULLIF(atual.flag_atual, 'None'), '')
    OR COALESCE(NULLIF(CAST(calculado.numero_entrada AS STRING), 'None'), '') != COALESCE(NULLIF(CAST(atual.numero_entrada AS STRING), 'None'), '')
    OR COALESCE(NULLIF(calculado.status_sinc, 'None'), '')           != COALESCE(NULLIF(atual.status_sinc, 'None'), '')
