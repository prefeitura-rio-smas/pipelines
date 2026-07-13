{{ config(materialized = 'ephemeral') }}

SELECT
  -- Chaves da ficha (layer 0)
  objectid,
  globalid,
  uniquerowid,

  -- Unidade de referência do equipamento
  unidade_calculo,
  unidade_bairro,
  unidade_cas,

  -- Dados da pessoa cadastrada (cabeçalho da ficha)
  nome_usuario,
  nome_social,
  data_nascimento,
  data_nascimento_iso,
  idade,
  faixa_etaria,
  cpf,
  calc_valido,
  motivo_cpf,
  estado_nascimento,
  migrante_sim_nao,
  nome_mae,
  nome_pai,
  grupo_familiar,
  raca_cor_etnia,
  sexo,

  -- Controles de exclusão e edição
  exclusao_unidade_calculo,
  exclusao_unidade_bairro,
  exclusao_unidade_cas,
  nome_usuario_ver,
  excluir_ficha,
  nome_tecnico_preenc_form,
  observacoes_edicao,
  data_preenc_form,
  data_exclusao,

  -- Flags de filtro e painel
  filtro_primeira_letra_equip,
  filtro_cinco_letra_equip,
  filtro_primeira_letra,
  filtro_data_abordagem,
  filtro_mes_ultima_abord,
  filtro_ano_ultima_abordagem,
  flag_painel,

  -- Metadados de auditoria
  created_user,
  created_date,
  last_edited_user,
  last_edited_date,
  nome_usuario_2,

  -- Timestamp de captura
  timestamp_captura

FROM {{ source('arcgis_raw', 'abordagem_raw') }}
