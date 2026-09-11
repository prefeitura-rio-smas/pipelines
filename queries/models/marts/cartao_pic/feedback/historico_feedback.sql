{{ config(
    materialized='incremental',
    unique_key='unique_id'
) }}

WITH 
delta_pic AS (
    SELECT 
        CAST(delta.objectid AS STRING) as objectid_origem,
        'entrega_pic' as produto,
        'verificacao' as coluna,
        raw.verificacao as valor_antigo,
        delta.verificacao as valor_novo
    FROM {{ ref('delta_feedback_pic') }} delta
    JOIN {{ source('arcgis_raw', 'primeira_infancia_carioca_raw') }} raw ON delta.objectid = raw.objectid
),

delta_controle_unpivot AS (
    SELECT
        CAST(objectid_origem AS STRING) as objectid_origem,
        'controle_cas' as produto,
        item.coluna,
        item.valor_antigo,
        item.valor_novo
    FROM (
        SELECT
            CAST(delta.objectid AS STRING) as objectid_origem,
            [
                STRUCT('cartao_entregue' as coluna, CAST(raw.cartao_entregue AS STRING) as valor_antigo, CAST(delta.cartao_entregue AS STRING) as valor_novo),
                STRUCT('local_entrega' as coluna, CAST(raw.local_entrega AS STRING) as valor_antigo, CAST(delta.local_entrega AS STRING) as valor_novo),
                STRUCT('data_entrega_text' as coluna, CAST(raw.data_entrega_text AS STRING) as valor_antigo, CAST(delta.data_entrega_text AS STRING) as valor_novo),
                STRUCT('resp_retirada' as coluna, CAST(raw.resp_retirada AS STRING) as valor_antigo, CAST(delta.resp_retirada AS STRING) as valor_novo),
                STRUCT('data_particao_retirada' as coluna, CAST(NULL AS STRING) as valor_antigo, CAST(delta.data_particao_retirada AS STRING) as valor_novo)
            ] as updates
        FROM {{ ref('delta_feedback_controle') }} delta
        JOIN {{ source('arcgis_raw', 'controle_cas_raw') }} raw ON delta.objectid = raw.objectid
    )
    CROSS JOIN UNNEST(updates) as item
    WHERE COALESCE(item.valor_antigo, '') != COALESCE(item.valor_novo, '')
),

unificado AS (
    SELECT * FROM delta_pic
    UNION ALL
    SELECT * FROM delta_controle_unpivot
)

SELECT 
    CURRENT_DATETIME('America/Sao_Paulo') as timestamp_execucao,
    FARM_FINGERPRINT(TO_JSON_STRING(u)) as unique_id,
    u.* 
FROM unificado u
