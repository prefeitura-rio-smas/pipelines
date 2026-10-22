-- ============================================================
-- Espelho cd_cidadao_pac do CadÚnico — export para o GCS
--
-- A transformação (DDL, joins, CASEs e filtro de negócio) vive no dbt:
--   queries/models/marts/risco_social/mart_cadunico__cidadao_pac.sql
--
-- Aqui só há o EXPORT DATA da tabela já materializada, no layout consumido
-- pelo app do prontuário: pasta shardada por data da partição, CSV gzip com
-- cabeçalho (mesmo contrato de antes — 31 colunas, nomes e ordem).
--
-- Placeholders preenchidos em pipelines/cadunico_espelho/tasks.py:
--   TARGET_TABLE → projeto.dataset.tabela (prod ou staging)
--   EXPORT_URI   → gs://bucket/prefixo/cd_cidadao_pac_<data>/cd_cidadao_pac_*.csv.gz
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
