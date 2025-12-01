{{ config(materialized='ephemeral') }}

WITH base_table AS (
  SELECT 
  id_usuario,
  beneficio,
  FROM rj-smas-dev.gerenciamento__dbt.tipo_beneficio
  WHERE beneficio = 'BPC-Benefício de Prestação Continuada' -- colocar o tipo de benefício 
  and id_usuario is not null
),

tabela_filtro AS (
SELECT
a.seqpac,
a.seqfamil,
a.parentesco_responsavel_familia,
b.id_usuario
from rj-smas.brutos_acolherio_staging.gh_familias_membros a 
inner JOIN base_table b on b.id_usuario = a.seqpac
where datsaida is null
),

filtro_unidade AS (
SELECT 
a.*,
c.dscus
FROM tabela_filtro a
INNER JOIN rj-smas.brutos_acolherio_staging.gh_cidadao_pac b ON a.id_usuario = b.seqpac
LEFT JOIN rj-smas.brutos_acolherio_staging.gh_us c ON b.sequsref = c.sequs
)

SELECT
dscus,
COUNT(*) AS quantidade_familia_bpc
FROM filtro_unidade
GROUP BY dscus
