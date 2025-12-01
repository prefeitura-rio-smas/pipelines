{{ config(materialized='table') }}

-- A1) Total de famílias inseridas no acompanhamento PAEFI.
WITH paefi_total AS (
SELECT
  seqfamil,
  datcancel,
  datcadastr,
  seqlogincad,
  seqservassist,
  seqlogincancel,
  seqfamilservassist
FROM rj-smas.brutos_acolherio_staging.gh_famil_servassist
WHERE seqservassist = 6 -- ACOMPANHAMENTO PAEFI
AND datcancel IS NULL
),

membro_familia  AS (
SELECT 
a.*,
b.seqmembro,
b.seqpac
from paefi_total a 
LEFT JOIN rj-smas.brutos_acolherio_staging.gh_familias_membros b ON a.seqfamil = b.seqfamil
),

responsavel_familiar AS (
SELECT * FROM membro_familia
WHERE seqmembro = 1
),

filtrando_unidade AS (
SELECT
a.seqfamil,
a.seqpac,
c.dscus
FROM responsavel_familiar a 
INNER JOIN rj-smas.brutos_acolherio_staging.gh_cidadao_pac b ON a.seqpac = b.seqpac
LEFT JOIN rj-smas.brutos_acolherio_staging.gh_us c ON b.sequsref = c.sequs
),

--Tabela final para join (A1)
paefi_total_sistema AS (
SELECT
dscus,
COUNT(*) AS paefi_total_sistema
FROM filtrando_unidade
GROUP BY dscus
),

-- Item A2
paefil_total_mes_11 AS (
  SELECT *
  FROM {{ ref('paefi_mes_atual')}}
),

membro_familia2  AS (
SELECT 
a.*,
b.seqmembro,
b.seqpac
from paefil_total_mes_11 a 
LEFT JOIN rj-smas.brutos_acolherio_staging.gh_familias_membros b ON a.seqfamil = b.seqfamil
),

responsavel_familiar2 AS (
SELECT * FROM membro_familia2
WHERE seqmembro = 1
),

filtro_unidade AS (
SELECT 
a.seqfamil,
a.seqpac,
b.sequsref,
c.dscus
FROM responsavel_familiar2 a 
INNER JOIN rj-smas.brutos_acolherio_staging.gh_cidadao_pac b ON a.seqpac = b.seqpac
LEFT JOIN rj-smas.brutos_acolherio_staging.gh_us c ON c.sequs = b.sequsref
),

-- Tabela final para join  (A2)
paefi_mes_atual_novos AS (
SELECT
 dscus,
COUNT(*)  AS paefi_total_novos_mes_atual
FROM filtro_unidade
GROUP BY dscus
)

--SELECT FINAL JUNTANDO AS DUAS TABELAS.
SELECT 
a.dscus,
b.paefi_total_novos_mes_atual,
c.paefi_total_sistema,
d.quantidade_familia_bolsa_familia,
e.quantidade_familia_bpc,
f.quantidade_familia_crianca_trab,
g.familias_vitimas_violencias_novas_paefi,
g.familias_crianca_violencias_sexual_paefi
FROM {{ source('brutos_acolherio_staging', 'gh_us') }} a
LEFT JOIN paefi_mes_atual_novos b ON a.dscus = b.dscus
LEFT JOIN paefi_total_sistema c ON a.dscus = c.dscus
LEFT JOIN {{ ref('familias_bolsa_familia_mes_atual')}} d ON a.dscus = d.dscus
LEFT JOIN {{ ref('familias_bpc_mes_atual')}} e ON a.dscus = e.dscus
LEFT JOIN {{ ref('familias_crianca_trab_infantil')}} f ON a.dscus = f.dscus
LEFT JOIN {{ ref('familias_vitimas_violencias_nova_paefi')}} g ON a.dscus = g.dscus
