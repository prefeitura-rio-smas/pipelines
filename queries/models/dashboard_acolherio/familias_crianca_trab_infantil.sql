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

filtro_unidade AS (
SELECT 
a.seqfamil,
a.seqpac,
c.dscus,
d.violacao_direito
FROM  membro_familia a
INNER JOIN rj-smas.brutos_acolherio_staging.gh_cidadao_pac b ON a.seqpac = b.seqpac
LEFT JOIN rj-smas.brutos_acolherio_staging.gh_us c ON b.sequsref = c.sequs
LEFT JOIN rj-smas-dev.dashboard_acolherio.violacao_direito d ON a.seqpac = d.id_usuario
)

SELECT
dscus,
COUNT(*) AS quantidade_familia_crianca_trab
FROM filtro_unidade
WHERE violacao_direito = "Trabalho Infatil"
GROUP BY dscus