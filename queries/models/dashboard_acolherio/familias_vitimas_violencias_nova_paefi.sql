{{ config(materialized='ephemeral') }}

WITH base_table AS (
SELECT
  indativo,
  seqfamil,
  datcancel,
  datcadastr,
  seqlogincad,
  seqservassist,
  seqlogincancel,
  seqfamilservassist,
  EXTRACT(MONTH FROM DATETIME(datcadastr)) AS mes_cadastro
FROM rj-smas.brutos_acolherio_staging.gh_famil_servassist
WHERE seqservassist = 6
AND datcancel IS NULL
),

filtro_mes AS (
  SELECT 
  *
  FROM base_table
  WHERE mes_cadastro = 11
),

membro_familia  AS (
SELECT 
a.*,
b.seqmembro,
b.seqpac
from filtro_mes a 
LEFT JOIN rj-smas.brutos_acolherio_staging.gh_familias_membros b ON a.seqfamil = b.seqfamil
),

teste AS (
SELECT 
a.*,
b.VIOLACAO_DIREITO
FROM membro_familia a
INNER JOIN rj-smas-dev.dashboard_acolherio.relatorio_geral b ON b.id_usuario = a.seqpac
),

filtro_novos_inseridos AS (
SELECT * FROM teste
WHERE violacao_direito != ""
AND violacao_direito != "N"
),


filtro_unidade AS (
SELECT 
a.*,
c.dscus,
b.datnascim AS data_nascimento
FROM filtro_novos_inseridos a
INNER JOIN rj-smas.brutos_acolherio_staging.gh_cidadao_pac b ON a.seqpac = b.seqpac
LEFT JOIN rj-smas.brutos_acolherio_staging.gh_us c ON b.sequsref = c.sequs
),

-- Item B1 do RMA CREAS.
familia_violencia_novo_paefi AS (
SELECT
dscus,
COUNT(*) AS familias_vitimas_violencias_novas_paefi
FROM filtro_unidade
GROUP BY dscus
),

crianca_adoslecentes_abuso_sexual AS (
  SELECT
  a.*,
  DATE_DIFF(CURRENT_DATE(), data_nascimento, YEAR) AS idade
  FROM filtro_unidade a
  INNER JOIN {{ source('dashboard_acolherio', 'violacao_direito') }} b ON a.seqpac = b.id_usuario
),

crianca_adoslecentes_abuso_sexual_filtrado AS (
  SELECT 
  * 
  FROM crianca_adoslecentes_abuso_sexual
  WHERE idade < 18
),

-- (C1) Total de familia com criança vitímas de violência sexual por unidade
crianca_adoslecentes_abuso_sexual_unidade_familia AS (
SELECT
dscus,
COUNT(*) AS familias_crianca_violencias_sexual_paefi
FROM crianca_adoslecentes_abuso_sexual_filtrado
GROUP BY dscus
)

--SELECT FINAL

SELECT 
a.dscus,
a.familias_vitimas_violencias_novas_paefi,
b.familias_crianca_violencias_sexual_paefi
FROM familia_violencia_novo_paefi a
INNER JOIN crianca_adoslecentes_abuso_sexual_unidade_familia b ON a.dscus = b.dscus