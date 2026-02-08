-- queries/models/pic/delta_feedback_controle.sql
-- Registros que precisam ser atualizados no ArcGIS do Controle CAS
SELECT 
    *
FROM {{ ref('controle_cas') }}
LIMIT 0 -- Preencher com a lógica de JOIN e filtro de diferença
