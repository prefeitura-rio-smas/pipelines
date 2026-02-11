-- queries/models/pic/delta_feedback_controle.sql
-- Registros que precisam ser atualizados no ArcGIS do Controle CAS

WITH 
calculado AS (
    SELECT 
        objectid,
        cartao_entregue,
        local_entrega,
        data_entrega_text,
        resp_retirada
    FROM {{ ref('controle_cas') }}
),

atual AS (
    SELECT 
        objectid,
        cartao_entregue,
        local_entrega,
        data_entrega_text,
        resp_retirada
    FROM {{ source('arcgis_raw', 'controle_cas_raw') }}
)

SELECT 
    calculado.*
FROM calculado
JOIN atual ON calculado.objectid = atual.objectid
WHERE 
    COALESCE(NULLIF(calculado.cartao_entregue, 'None'), '')   != COALESCE(NULLIF(atual.cartao_entregue, 'None'), '') OR
    COALESCE(NULLIF(calculado.local_entrega, 'None'), '')    != COALESCE(NULLIF(atual.local_entrega, 'None'), '') OR
    COALESCE(NULLIF(calculado.data_entrega_text, 'None'), '') != COALESCE(NULLIF(atual.data_entrega_text, 'None'), '') OR
    COALESCE(NULLIF(calculado.resp_retirada, 'None'), '')     != COALESCE(NULLIF(atual.resp_retirada, 'None'), '')