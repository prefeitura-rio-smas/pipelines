{{ config(materialized='ephemeral') }}

-- Total de usuários em companhamento PAEFI.

WITH paefi_total AS (
SELECT
  indativo,
  seqfamil,
  datcancel,
  datcadastr,
  seqlogincad,
  seqservassist,
  seqlogincancel,
  seqfamilservassist,
  EXTRACT(MONTH FROM DATETIME(datcadastr)) AS mes,
  FORMAT_DATETIME("%H:%M", datcadastr) AS hora_formatada
FROM rj-smas.brutos_acolherio_staging.gh_famil_servassist
WHERE seqservassist = 6
AND datcancel IS NULL
)

SELECT * FROM paefi_total
-- WHERE mes = EXTRACT(MONTH FROM CURRENT_DATE) Busca os dados do mês atual
WHERE mes = 11
