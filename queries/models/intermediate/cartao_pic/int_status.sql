-- Intermediate: status empilhada do Cartão PIC (base estática por folha)
-- Grão: 1 linha por (data_particao, cod_familiar_fam).
--
-- Monta as partições históricas (snapshots antigas) + a folha nova
-- (cartao_pic.beneficiarios) com os ATRIBUTOS ESTÁTICOS POR FOLHA:
-- classificação (acao_smas, alerta), cohort, ativo/inativo (família e RF),
-- filtro_email_cas, flags e endereço.
--
-- Os atributos DINÂMICOS (Survey, eventos, status de entrega) ficam na
-- mart_status (atualizada de hora em hora).
--
-- Partições:
--   2025-08-08 ← status_202508 (29.501)          + colunas novas = NULL
--   2026-02-13 ← status_completo_202602 (29.999)  + NULL
--   2026-04-10 ← status_completo_202604 (30.000)  + NULL
--   2026-07-10 ← cartao_pic.beneficiarios (30.000) + schema completo

{{ config(
    materialized='table',
    tags=['cartao_pic_status'],
) }}

-- ============================================================
-- PARTIÇÃO 1: folha ago/2025 (status_202508) — colunas novas NULL
-- ============================================================
SELECT
  DATE('2025-08-08') AS data_particao,
  CAST(COD_FAMILIAR_FAM AS STRING) AS cod_familiar_fam,
  NUM_CPF_RESPONSAVEL AS num_cpf_responsavel,
  REGEXP_REPLACE(NUM_CPF_RESPONSAVEL, r'(\d{3})(\d{3})(\d{3})(\d{2})', r'\1.\2.\3-\4') AS cpf_formatado,
  ENVELOPE AS envelope,
  NOME_RESPONSAVEL AS nome_responsavel,
  NOME_SOCIAL_RESPONSAVEL AS nome_social_responsavel,
  SAFE_CAST(IDADE_RESPONSAVEL AS INT64) AS idade_responsavel,
  CAST(NULL AS STRING) AS faixa_etaria_responsavel,
  COD_SEXO_RESPONSAVEL AS cod_sexo_responsavel,
  SAFE_CAST(QTD_CRIANCAS_0_A_4_ANOS AS INT64) AS qtd_criancas_0_a_4_anos,
  CAS AS cas,
  BAIRRO AS bairro,
  NOM_UNIDADE_TERRITORIAL_FAM AS nom_unidade_territorial_fam,
  CONCAT(
    COALESCE(NOM_TIP_LOGRADOURO_FAM, ''), ' ',
    COALESCE(NOM_TITULO_LOGRADOURO_FAM, ''), ' ',
    COALESCE(NOM_LOGRADOURO_FAM, ''),
    COALESCE(CONCAT(', ', NUM_LOGRADOURO_FAM), '')
  ) AS endereco_fam,
  DES_COMPLEMENTO_FAM AS des_complemento_fam,
  DES_COMPLEMENTO_ADIC_FAM AS desc_complemento_adic_fam,
  TXT_REFERENCIA_LOCAL_FAM AS txt_referencia_local_fam,
  NUM_TEL_CONTATO_1_FAM AS num_tel_contato_1_fam,
  NUM_TEL_CONTATO_2_FAM AS num_tel_contato_2_fam,
  NOM_CENTRO_ASSIST_FAM AS nom_centro_assist_fam,
  CAST(NULL AS INT64) AS flag_folha_anterior,
  CAST(NULL AS INT64) AS flag_familia_nao_encontrada,
  CAST(NULL AS INT64) AS flag_mudanca_rf,
  CAST(NULL AS INT64) AS flag_vr_correcao,
  CAST(NULL AS STRING) AS status,
  CAST(NULL AS INT64) AS flag_recebe_envelope,
  CAST(NULL AS STRING) AS situacao,
  CAST(NULL AS BOOL) AS mudou,
  CAST(NULL AS STRING) AS cod_familiar_fam_origem,
  CAST(NULL AS INT64) AS flag_ausencia_doc_civil,
  CAST(NULL AS INT64) AS flag_reconcessao,
  CAST(NULL AS STRING) AS acao_smas,
  CAST(NULL AS STRING) AS alerta,
  CASE
    WHEN CAS = '01' THEN 'cas1@prefeitura.rio'
    WHEN CAS = '02' THEN 'cas2@prefeitura.rio'
    WHEN CAS = '03' THEN 'cas3@prefeitura.rio'
    WHEN CAS = '04' THEN 'cas4@prefeitura.rio'
    WHEN CAS = '05' THEN 'cas5@prefeitura.rio'
    WHEN CAS = '06' THEN 'cas6@prefeitura.rio'
    WHEN CAS = '07' THEN 'cas7@prefeitura.rio'
    WHEN CAS = '08' THEN 'cas8@prefeitura.rio'
    WHEN CAS = '09' THEN 'cas9@prefeitura.rio'
    WHEN CAS = '10' THEN 'cas10@prefeitura.rio'
  END AS filtro_email_cas,
  CAST(NULL AS ARRAY<DATE>) AS cohort,
  CAST(NULL AS DATE) AS cohort_entrada,
  CAST(NULL AS INT64) AS qtd_folhas,
  CAST(NULL AS STRING) AS ativo_inativo_familia,
  CAST(NULL AS STRING) AS motivo_inativo_familia,
  CAST(NULL AS STRING) AS ativo_inativo_rf,
  CAST(NULL AS STRING) AS motivo_inativo_rf
FROM {{ source('pic_snapshots', 'cartao_primeira_infancia_carioca_status_202508') }}

UNION ALL

-- ============================================================
-- PARTIÇÃO 2: folha fev/2026 (status_completo_202602) — colunas novas NULL
-- ============================================================
SELECT
  DATE('2026-02-13') AS data_particao,
  CAST(COD_FAMILIAR_FAM AS STRING) AS cod_familiar_fam,
  NUM_CPF_RESPONSAVEL AS num_cpf_responsavel,
  REGEXP_REPLACE(NUM_CPF_RESPONSAVEL, r'(\d{3})(\d{3})(\d{3})(\d{2})', r'\1.\2.\3-\4') AS cpf_formatado,
  ENVELOPE AS envelope,
  NOME_RESPONSAVEL AS nome_responsavel,
  NOME_SOCIAL_RESPONSAVEL AS nome_social_responsavel,
  SAFE_CAST(Idade_RESPONSAVEL AS INT64) AS idade_responsavel,
  CAST(NULL AS STRING) AS faixa_etaria_responsavel,
  COD_SEXO_RESPONSAVEL AS cod_sexo_responsavel,
  SAFE_CAST(QTD_CRIANCAS_0_A_4_ANOS AS INT64) AS qtd_criancas_0_a_4_anos,
  CAS AS cas,
  BAIRRO AS bairro,
  NOM_UNIDADE_TERRITORIAL_FAM AS nom_unidade_territorial_fam,
  CONCAT(
    COALESCE(NOM_TIP_LOGRADOURO_FAM, ''), ' ',
    COALESCE(NOM_TITULO_LOGRADOURO_FAM, ''), ' ',
    COALESCE(NOM_LOGRADOURO_FAM, ''),
    COALESCE(CONCAT(', ', NUM_LOGRADOURO_FAM), '')
  ) AS endereco_fam,
  DES_COMPLEMENTO_FAM AS des_complemento_fam,
  DES_COMPLEMENTO_ADIC_FAM AS desc_complemento_adic_fam,
  TXT_REFERENCIA_LOCAL_FAM AS txt_referencia_local_fam,
  NUM_TEL_CONTATO_1_FAM AS num_tel_contato_1_fam,
  NUM_TEL_CONTATO_2_FAM AS num_tel_contato_2_fam,
  NOM_CENTRO_ASSIST_FAM AS nom_centro_assist_fam,
  CAST(NULL AS INT64) AS flag_folha_anterior,
  CAST(NULL AS INT64) AS flag_familia_nao_encontrada,
  CAST(NULL AS INT64) AS flag_mudanca_rf,
  CAST(NULL AS INT64) AS flag_vr_correcao,
  CAST(NULL AS STRING) AS status,
  CAST(NULL AS INT64) AS flag_recebe_envelope,
  CAST(NULL AS STRING) AS situacao,
  CAST(NULL AS BOOL) AS mudou,
  CAST(NULL AS STRING) AS cod_familiar_fam_origem,
  CAST(NULL AS INT64) AS flag_ausencia_doc_civil,
  CAST(NULL AS INT64) AS flag_reconcessao,
  CAST(NULL AS STRING) AS acao_smas,
  CAST(NULL AS STRING) AS alerta,
  CASE
    WHEN CAS = '01' THEN 'cas1@prefeitura.rio'
    WHEN CAS = '02' THEN 'cas2@prefeitura.rio'
    WHEN CAS = '03' THEN 'cas3@prefeitura.rio'
    WHEN CAS = '04' THEN 'cas4@prefeitura.rio'
    WHEN CAS = '05' THEN 'cas5@prefeitura.rio'
    WHEN CAS = '06' THEN 'cas6@prefeitura.rio'
    WHEN CAS = '07' THEN 'cas7@prefeitura.rio'
    WHEN CAS = '08' THEN 'cas8@prefeitura.rio'
    WHEN CAS = '09' THEN 'cas9@prefeitura.rio'
    WHEN CAS = '10' THEN 'cas10@prefeitura.rio'
  END AS filtro_email_cas,
  CAST(NULL AS ARRAY<DATE>) AS cohort,
  CAST(NULL AS DATE) AS cohort_entrada,
  CAST(NULL AS INT64) AS qtd_folhas,
  CAST(NULL AS STRING) AS ativo_inativo_familia,
  CAST(NULL AS STRING) AS motivo_inativo_familia,
  CAST(NULL AS STRING) AS ativo_inativo_rf,
  CAST(NULL AS STRING) AS motivo_inativo_rf
FROM {{ source('pic_snapshots', 'cartao_primeira_infancia_carioca_status_completo_202602') }}

UNION ALL

-- ============================================================
-- PARTIÇÃO 3: folha abr/2026 (status_completo_202604) — colunas novas NULL
-- ============================================================
SELECT
  DATE('2026-04-10') AS data_particao,
  CAST(COD_FAMILIAR_FAM AS STRING) AS cod_familiar_fam,
  NUM_CPF_RESPONSAVEL AS num_cpf_responsavel,
  REGEXP_REPLACE(NUM_CPF_RESPONSAVEL, r'(\d{3})(\d{3})(\d{3})(\d{2})', r'\1.\2.\3-\4') AS cpf_formatado,
  ENVELOPE AS envelope,
  NOME_RESPONSAVEL AS nome_responsavel,
  NOME_SOCIAL_RESPONSAVEL AS nome_social_responsavel,
  SAFE_CAST(Idade_RESPONSAVEL AS INT64) AS idade_responsavel,
  CAST(NULL AS STRING) AS faixa_etaria_responsavel,
  COD_SEXO_RESPONSAVEL AS cod_sexo_responsavel,
  SAFE_CAST(QTD_CRIANCAS_0_A_4_ANOS AS INT64) AS qtd_criancas_0_a_4_anos,
  CAS AS cas,
  BAIRRO AS bairro,
  NOM_UNIDADE_TERRITORIAL_FAM AS nom_unidade_territorial_fam,
  CONCAT(
    COALESCE(NOM_TIP_LOGRADOURO_FAM, ''), ' ',
    COALESCE(NOM_TITULO_LOGRADOURO_FAM, ''), ' ',
    COALESCE(NOM_LOGRADOURO_FAM, ''),
    COALESCE(CONCAT(', ', NUM_LOGRADOURO_FAM), '')
  ) AS endereco_fam,
  DES_COMPLEMENTO_FAM AS des_complemento_fam,
  DES_COMPLEMENTO_ADIC_FAM AS desc_complemento_adic_fam,
  TXT_REFERENCIA_LOCAL_FAM AS txt_referencia_local_fam,
  NUM_TEL_CONTATO_1_FAM AS num_tel_contato_1_fam,
  NUM_TEL_CONTATO_2_FAM AS num_tel_contato_2_fam,
  NOM_CENTRO_ASSIST_FAM AS nom_centro_assist_fam,
  CAST(NULL AS INT64) AS flag_folha_anterior,
  CAST(NULL AS INT64) AS flag_familia_nao_encontrada,
  CAST(NULL AS INT64) AS flag_mudanca_rf,
  CAST(NULL AS INT64) AS flag_vr_correcao,
  CAST(NULL AS STRING) AS status,
  CAST(NULL AS INT64) AS flag_recebe_envelope,
  CAST(NULL AS STRING) AS situacao,
  CAST(NULL AS BOOL) AS mudou,
  CAST(NULL AS STRING) AS cod_familiar_fam_origem,
  CAST(NULL AS INT64) AS flag_ausencia_doc_civil,
  CAST(NULL AS INT64) AS flag_reconcessao,
  CAST(NULL AS STRING) AS acao_smas,
  CAST(NULL AS STRING) AS alerta,
  CASE
    WHEN CAS = '01' THEN 'cas1@prefeitura.rio'
    WHEN CAS = '02' THEN 'cas2@prefeitura.rio'
    WHEN CAS = '03' THEN 'cas3@prefeitura.rio'
    WHEN CAS = '04' THEN 'cas4@prefeitura.rio'
    WHEN CAS = '05' THEN 'cas5@prefeitura.rio'
    WHEN CAS = '06' THEN 'cas6@prefeitura.rio'
    WHEN CAS = '07' THEN 'cas7@prefeitura.rio'
    WHEN CAS = '08' THEN 'cas8@prefeitura.rio'
    WHEN CAS = '09' THEN 'cas9@prefeitura.rio'
    WHEN CAS = '10' THEN 'cas10@prefeitura.rio'
  END AS filtro_email_cas,
  CAST(NULL AS ARRAY<DATE>) AS cohort,
  CAST(NULL AS DATE) AS cohort_entrada,
  CAST(NULL AS INT64) AS qtd_folhas,
  CAST(NULL AS STRING) AS ativo_inativo_familia,
  CAST(NULL AS STRING) AS motivo_inativo_familia,
  CAST(NULL AS STRING) AS ativo_inativo_rf,
  CAST(NULL AS STRING) AS motivo_inativo_rf
FROM {{ source('pic_snapshots', 'cartao_primeira_infancia_carioca_status_completo_202604') }}

UNION ALL

-- ============================================================
-- PARTIÇÃO 4: folha nova (2026-07-10) — cartao_pic.beneficiarios + schema completo
-- ============================================================
SELECT
  b.data_particao,
  b.cod_familiar_fam,
  b.num_cpf_responsavel,
  b.cpf_formatado,
  CAST(b.envelope AS STRING) AS envelope,
  b.nome_responsavel,
  CAST(NULL AS STRING) AS nome_social_responsavel,
  b.idade AS idade_responsavel,
  CASE
    WHEN b.idade < 18 THEN '0 a 17 anos'
    WHEN b.idade < 30 THEN '18 a 29 anos'
    WHEN b.idade < 40 THEN '30 a 39 anos'
    WHEN b.idade < 50 THEN '40 a 49 anos'
    WHEN b.idade < 60 THEN '50 a 59 anos'
    ELSE '60 anos ou mais'
  END AS faixa_etaria_responsavel,
  b.cod_sexo_responsavel,
  b.qtd_criancas_0_a_4_anos,
  b.cas,
  b.bairro,
  b.nom_unidade_territorial_fam,
  b.endereco_fam,
  b.des_complemento_fam,
  b.desc_complemento_adic_fam,
  b.txt_referencia_local_fam,
  CONCAT('55', SUBSTR(b.num_tel_contato_1_fam, 1, 2), REGEXP_REPLACE(SUBSTR(b.num_tel_contato_1_fam, 3), r'^0+', '')) AS num_tel_contato_1_fam,
  CONCAT('55', SUBSTR(b.num_tel_contato_2_fam, 1, 2), REGEXP_REPLACE(SUBSTR(b.num_tel_contato_2_fam, 3), r'^0+', '')) AS num_tel_contato_2_fam,
  b.nom_centro_assist_fam,
  b.flag_folha_anterior,
  b.flag_remocao_decisao_administrativa AS flag_familia_nao_encontrada,
  b.flag_mudanca_rf,
  b.flag_vr_correcao,
  b.familia.status AS status,
  b.flag_recebe_envelope,
  b.familia.situacao AS situacao,
  b.responsavel_familiar.mudou AS mudou,
  b.responsavel_familiar.cod_familiar_fam_origem AS cod_familiar_fam_origem,
  b.responsavel_familiar.ausencia_documentacao AS flag_ausencia_doc_civil,
  CASE
    WHEN (b.cartao_retirado = 'sim' AND b.familia.situacao = 'reconcessao'
          AND b.retorno_apos_atualizacao = 'nao' AND b.responsavel_familiar.ausencia_documentacao = 0)
      OR (b.cartao_retirado = 'sim' AND b.responsavel_familiar.situacao_rf = 'rf_nova_familia')
      THEN 1 ELSE 0
  END AS flag_reconcessao,
  -- acao_smas: classificação (recorte de monitoramento = != 'mantidos')
  CASE
    WHEN b.flag_recebe_envelope = 1 AND b.flag_mudanca_rf IN (0, 1) THEN 'novos_beneficiarios'
    WHEN b.retorno_apos_atualizacao = 'sim'
      OR (b.cartao_retirado = 'nao' AND b.responsavel_familiar.situacao_rf = 'rf_nova_familia')
      THEN 'retorno_apos_atualizacao'
    WHEN b.responsavel_familiar.ausencia_documentacao = 1 THEN 'ausencia_documentacao_obrigatoria'
    WHEN (b.cartao_retirado = 'sim' AND b.familia.situacao = 'reconcessao'
          AND b.retorno_apos_atualizacao = 'nao' AND b.responsavel_familiar.ausencia_documentacao = 0)
      OR (b.cartao_retirado = 'sim' AND b.responsavel_familiar.situacao_rf = 'rf_nova_familia')
      THEN 'reconcessao'
    ELSE 'mantidos'
  END AS acao_smas,
  -- alerta: destino operacional (eventos/mensagens), derivado da acao_smas
  CASE
    WHEN b.flag_recebe_envelope = 1 AND b.flag_mudanca_rf IN (0, 1) THEN 'retirar_cartao'
    WHEN b.retorno_apos_atualizacao = 'sim'
      OR (b.cartao_retirado = 'nao' AND b.responsavel_familiar.situacao_rf = 'rf_nova_familia')
      THEN 'retirar_cartao_apos_atualizacao'
    WHEN b.responsavel_familiar.ausencia_documentacao = 1 THEN 'retirar_cartao_regularizacao_documento'
    WHEN (b.cartao_retirado = 'sim' AND b.familia.situacao = 'reconcessao'
          AND b.retorno_apos_atualizacao = 'nao' AND b.responsavel_familiar.ausencia_documentacao = 0)
      OR (b.cartao_retirado = 'sim' AND b.responsavel_familiar.situacao_rf = 'rf_nova_familia')
      THEN 'aviso'
    ELSE NULL
  END AS alerta,
  CASE
    WHEN b.cas = '01' THEN 'cas1@prefeitura.rio'
    WHEN b.cas = '02' THEN 'cas2@prefeitura.rio'
    WHEN b.cas = '03' THEN 'cas3@prefeitura.rio'
    WHEN b.cas = '04' THEN 'cas4@prefeitura.rio'
    WHEN b.cas = '05' THEN 'cas5@prefeitura.rio'
    WHEN b.cas = '06' THEN 'cas6@prefeitura.rio'
    WHEN b.cas = '07' THEN 'cas7@prefeitura.rio'
    WHEN b.cas = '08' THEN 'cas8@prefeitura.rio'
    WHEN b.cas = '09' THEN 'cas9@prefeitura.rio'
    WHEN b.cas = '10' THEN 'cas10@prefeitura.rio'
  END AS filtro_email_cas,
  b.datas_participacao AS cohort,
  b.cohort_entrada,
  b.qtd_folhas,
  CASE WHEN b.familia.status IN ('mantida', 'nova') THEN 'ativo' ELSE 'inativo' END AS ativo_inativo_familia,
  CASE WHEN b.familia.status IN ('mantida', 'nova') THEN NULL ELSE b.familia.status END AS motivo_inativo_familia,
  CASE WHEN b.responsavel_familiar.situacao_rf IN ('rf_mantido','rf_substituido','rf_nova_familia','rf_inedito') THEN 'ativo' ELSE 'inativo' END AS ativo_inativo_rf,
  CASE WHEN b.responsavel_familiar.situacao_rf IN ('rf_mantido','rf_substituido','rf_nova_familia','rf_inedito') THEN NULL ELSE b.responsavel_familiar.situacao_rf END AS motivo_inativo_rf
FROM {{ source('cartao_pic', 'beneficiarios') }} b