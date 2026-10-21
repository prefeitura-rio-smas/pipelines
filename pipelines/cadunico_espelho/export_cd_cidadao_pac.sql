-- ============================================================
-- Espelho cd_cidadao_pac do CadÚnico
--
-- Materializa o espelho no BigQuery e exporta o CSV.gz para o GCS.
--
-- Custo: cada CTE filtra `data_particao = @particao_*` (query parameter).
-- Com o valor conhecido em tempo de execução, o BigQuery faz partition
-- pruning e lê somente a partição do mês (antes: MAX(data_particao) como
-- subquery, que impedia o pruning e varria as 54 partições de cada tabela).
--
-- Placeholders preenchidos em pipelines/cadunico_espelho/tasks.py:
--   TARGET_TABLE → projeto.dataset.tabela de destino (prod ou staging)
--   EXPORT_URI   → gs://bucket/prefixo/cd_cidadao_pac_<data>/cd_cidadao_pac_*.csv.gz
-- ============================================================

-- ============================================================
-- 1. Cria/atualiza a tabela de cidadãos do CadÚnico
-- ============================================================

CREATE OR REPLACE TABLE `{{TARGET_TABLE}}` AS

WITH i AS (
  SELECT
    nis_original AS dsccns,
    nome AS dscnomepac,
    nome_social_pessoa AS dscnomsoci,
    filiacao_1_nom_completo_mae_pessoa AS dscnmmae,
    filiacao_2 AS dscnmpai,
    LEFT(pais_nascimento, 5) AS paisorigem,
    municipio_nascimento AS muninasc,
    data_nascimento AS datnascim,

    CASE
      WHEN sexo = 'Masculino' THEN 'M'
      WHEN sexo = 'Feminino' THEN 'F'
    END AS indsexo,

    CASE
      WHEN id_tipo_identidade_genero = '002' THEN '06'
      WHEN id_tipo_identidade_genero = '001' THEN '05'
      WHEN id_tipo_identidade_genero = '003' THEN '04'
      ELSE '99'
    END AS indgenero,

    CASE
      WHEN id_raca_cor = '1' THEN '01'
      WHEN id_raca_cor = '2' THEN '02'
      WHEN id_raca_cor = '3' THEN '04'
      WHEN id_raca_cor = '4' THEN '03'
      WHEN id_raca_cor = '5' THEN '05'
      ELSE '99'
    END AS racacor,

    parentesco_responsavel_familia,

    CASE
      WHEN parentesco_responsavel_familia =
        'Pessoa Responsável pela Unidade Familiar - RF'
      THEN TRUE
      ELSE FALSE
    END AS flag_rf,

    estado_cadastral,
    id_familia,
    id_membro_familia,
    data_particao

  FROM `rj-smas.protecao_social_cadunico.identificacao_primeira_pessoa`

  WHERE data_particao = @particao_pessoa
),

-- ============================================================
-- 2. Identificação e controle do endereço familiar
-- ============================================================

ic AS (
  SELECT
    cep AS numcepender,
    LEFT(localidade, 40) AS dscbairroender,
    'RJ' AS sigufender,

    CONCAT(
      COALESCE(tipo_logradouro, ''),
      ' ',
      COALESCE(titulo_logradouro, ''),
      ' ',
      COALESCE(logradouro, '')
    ) AS dslogradouro,

    LEFT(CAST(numero_logradouro AS STRING), 7) AS numend,
    complemento AS complend,
    estado_cadastro,
    data_atualizacao,
    id_familia

  FROM `rj-smas.protecao_social_cadunico.identificacao_controle`

  WHERE data_particao = @particao_controle
),

-- ============================================================
-- 3. Dados de contato familiar
-- ============================================================

c AS (
  SELECT
    NULLIF(
      CONCAT(
        LTRIM(contato_ddd, '0'),
        LTRIM(contato_telefone, '0')
      ),
      ''
    ) AS dsctelef1,

    NULLIF(
      CONCAT(
        LTRIM(contato_2_ddd, '0'),
        LTRIM(contato_2_telefone, '0')
      ),
      ''
    ) AS dsctelef2,

    id_familia

  FROM `rj-smas.protecao_social_cadunico.contato`

  WHERE data_particao = @particao_contato
),

-- ============================================================
-- 4. Documentos pessoais
-- ============================================================

d AS (
  SELECT
    cpf AS numcpfpac,
    RIGHT(rg, 15) AS numrg,
    id_membro_familia

  FROM `rj-smas.protecao_social_cadunico.documento_pessoa`

  WHERE data_particao = @particao_documento
),

-- ============================================================
-- 5. Dados de deficiência
-- ============================================================

pd AS (
  SELECT
    CASE
      WHEN tem_deficiencia = 'Sim' THEN 'S'
      WHEN tem_deficiencia = 'Não' THEN 'N'
    END AS indpresdefi,

    id_membro_familia

  FROM `rj-smas.protecao_social_cadunico.pessoa_deficiencia`

  WHERE data_particao = @particao_deficiencia
)

-- ============================================================
-- 6. Consulta final
-- ============================================================

SELECT
  -- Identificação da pessoa
  i.dsccns,
  i.dscnomepac,
  i.dscnomsoci,
  i.indgenero,
  i.paisorigem,
  i.muninasc,
  i.dscnmmae,
  i.dscnmpai,
  i.datnascim,
  i.indsexo,
  i.racacor,
  i.parentesco_responsavel_familia,
  i.flag_rf,

  -- Documentos pessoais
  d.numcpfpac,
  d.numrg,

  -- Contatos familiares
  c.dsctelef1,
  c.dsctelef2,

  -- Endereço familiar
  ic.numcepender,
  ic.dslogradouro,
  ic.numend,
  ic.complend,
  ic.dscbairroender,
  ic.sigufender,
  ic.estado_cadastro,
  ic.data_atualizacao,

  -- Deficiência
  pd.indpresdefi,

  -- Chaves de identificação
  i.id_familia,
  i.id_membro_familia,

  -- Indicadores fixos
  'Sim' AS flag_cadunico,
  'Rio de Janeiro' AS muniend,

  -- Data da partição de identificação
  i.data_particao

FROM i

LEFT JOIN c
  ON i.id_familia = c.id_familia

LEFT JOIN ic
  ON i.id_familia = ic.id_familia

LEFT JOIN d
  ON i.id_membro_familia = d.id_membro_familia

LEFT JOIN pd
  ON i.id_membro_familia = pd.id_membro_familia

WHERE
  i.estado_cadastral != 'Excluido'
  AND ic.estado_cadastro = 'Cadastrado'
;

-- ============================================================
-- 7. Exporta para o bucket GCS
--    Pasta identificada pela data da partição (contrato do app)
-- ============================================================

EXPORT DATA OPTIONS (
  uri = '{{EXPORT_URI}}',
  format = 'CSV',
  compression = 'GZIP',
  overwrite = true,
  header = true,
  field_delimiter = ','
) AS
SELECT *
FROM `{{TARGET_TABLE}}`
;
