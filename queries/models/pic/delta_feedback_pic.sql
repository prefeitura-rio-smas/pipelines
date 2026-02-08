-- queries/models/pic/delta_feedback_pic.sql
-- Registros que precisam ser atualizados no ArcGIS da Primeira Infância
SELECT 
    *
FROM {{ ref('primeira_infancia_carioca') }}
LIMIT 0 -- Preencher com a lógica de JOIN e filtro de diferença
