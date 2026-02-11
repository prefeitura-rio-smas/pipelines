{{ config(
    materialized='incremental',
    unique_key='unique_id'
) }}

WITH 
delta_pic AS (
    SELECT 
        delta.objectid as objectid_origem,
        'entrega_pic' as produto,
        'verificacao' as coluna,
        raw.verificacao as valor_antigo,
        delta.verificacao as valor_novo
    FROM {{ ref('delta_feedback_pic') }} delta
    JOIN {{ source('arcgis_raw', 'primeira_infancia_carioca_raw') }} raw ON delta.objectid = raw.objectid
),

delta_controle_unpivot AS (
    SELECT 
        objectid_origem,
        'controle_cas' as produto,
        item.coluna,
        item.valor_antigo,
        item.valor_novo
    FROM (
        SELECT 
            delta.objectid as objectid_origem,
            [
                STRUCT('cartao_entregue' as coluna, raw.cartao_entregue as valor_antigo, delta.cartao_entregue as valor_novo),
                STRUCT('local_entrega' as coluna, raw.local_entrega as valor_antigo, delta.local_entrega as valor_novo),
                STRUCT('data_entrega_text' as coluna, raw.data_entrega_text as valor_antigo, delta.data_entrega_text as valor_novo),
                STRUCT('resp_retirada' as coluna, raw.resp_retirada as valor_antigo, delta.resp_retirada as valor_novo)
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

{% if is_incremental() %}
  -- this filter will only be applied on an incremental run
  WHERE timestamp_execucao > (SELECT MAX(timestamp_execucao) FROM {{ this }})
{% endif %}
