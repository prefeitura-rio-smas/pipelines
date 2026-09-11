-- queries/models/pic/delta_feedback_pic.sql
-- Registros que precisam ter a 'verificacao' atualizada no ArcGIS da Primeira Infancia

WITH 
calculado AS (
    SELECT 
        objectid,
        verificacao
    FROM {{ ref('primeira_infancia_carioca') }}
),

atual AS (
    SELECT 
        objectid,
        verificacao
    FROM {{ source('arcgis_raw', 'primeira_infancia_carioca_raw') }}
)

SELECT 
    calculado.objectid,
    calculado.verificacao
FROM calculado
JOIN atual ON calculado.objectid = atual.objectid
WHERE 
    COALESCE(calculado.verificacao, '') != COALESCE(atual.verificacao, '')