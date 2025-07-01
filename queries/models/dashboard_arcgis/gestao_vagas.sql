{{ config(materialized = 'table') }}

WITH clusters AS (
    {{ tratamento_duplicatas_cluster(
         source_relation   = source('arcgis_raw','gestao_vagas_repeat_raw'),
         col_globalid      = 'globalid',
         col_id_principal  = 'parentrowid'
    ) }}
)

SELECT
    r.*,
    c.nome_usuario_norm,
    c.nome_mae_norm,
    c.cluster_id,
    c.cluster_size,
    c.is_duplicado
FROM {{ ref('gestao_vagas_repeat') }}  r
LEFT JOIN clusters c
  ON r.globalid = c.globalid
