-- ============================================================
-- 1. Obtém a data da partição mais recente do CadÚnico
-- ============================================================

DECLARE v_data_particao DATE DEFAULT (
  SELECT CAST(MAX(data_particao) AS DATE)
  FROM `rj-smas.protecao_social_cadunico.identificacao_primeira_pessoa`
);

-- ============================================================
-- 2. Cria/atualiza a tabela de cidadãos do CadÚnico
-- ============================================================

CREATE OR REPLACE TABLE `rj-smas-dev.acolherio.cd_cidadao_pac` AS

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

  WHERE data_particao = (
    SELECT MAX(data_particao)
    FROM `rj-smas.protecao_social_cadunico.identificacao_primeira_pessoa`
  )
),

-- ============================================================
-- 3. Identificação e controle do endereço familiar
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

  WHERE data_particao = (
    SELECT MAX(data_particao)
    FROM `rj-smas.protecao_social_cadunico.identificacao_controle`
  )
),

-- ============================================================
-- 4. Dados de contato familiar
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

  WHERE data_particao = (
    SELECT MAX(data_particao)
    FROM `rj-smas.protecao_social_cadunico.contato`
  )
),

-- ============================================================
-- 5. Documentos pessoais
-- ============================================================

d AS (
  SELECT
    cpf AS numcpfpac,
    RIGHT(rg, 15) AS numrg,
    id_membro_familia

  FROM `rj-smas.protecao_social_cadunico.documento_pessoa`

  WHERE data_particao = (
    SELECT MAX(data_particao)
    FROM `rj-smas.protecao_social_cadunico.documento_pessoa`
  )
),

-- ============================================================
-- 6. Dados de deficiência
-- ============================================================

pd AS (
  SELECT
    CASE
      WHEN tem_deficiencia = 'Sim' THEN 'S'
      WHEN tem_deficiencia = 'Não' THEN 'N'
    END AS indpresdefi,

    id_membro_familia

  FROM `rj-smas.protecao_social_cadunico.pessoa_deficiencia`

  WHERE data_particao = (
    SELECT MAX(data_particao)
    FROM `rj-smas.protecao_social_cadunico.pessoa_deficiencia`
  )
)

-- ============================================================
-- 7. Consulta final
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
-- 8. Exporta para o bucket GCS
--    Pasta identificada pelo projeto e data da partição
-- ============================================================

EXECUTE IMMEDIATE FORMAT(
  """
  EXPORT DATA OPTIONS (
    uri = 'gs://rj-smas-dev/raw/cadunico/cd_cidadao_pac_%s/cd_cidadao_pac_*.csv.gz',
    format = 'CSV',
    compression = 'GZIP',
    overwrite = true,
    header = true,
    field_delimiter = ','
  ) AS
  SELECT *
  FROM `rj-smas-dev.acolherio.cd_cidadao_pac`
  """,
  FORMAT_DATE('%Y-%m-%d', v_data_particao)
);
