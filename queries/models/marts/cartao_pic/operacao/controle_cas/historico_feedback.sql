{{ config(
    materialized='incremental',
    unique_key='unique_id'
) }}

WITH
delta_pic AS (
    SELECT
        CAST(delta.objectid AS STRING) AS objectid_origem,
        'entrega_pic' AS produto,
        'verificacao' AS coluna,
        raw.verificacao AS valor_antigo,
        delta.verificacao AS valor_novo
    FROM {{ ref('delta_feedback_pic') }} AS delta
    INNER JOIN {{ source('arcgis_raw', 'primeira_infancia_carioca_raw') }} AS raw ON delta.objectid = raw.objectid
),

delta_controle_unpivot AS (
    SELECT
        CAST(objectid_origem AS STRING) AS objectid_origem,
        'controle_cas' AS produto,
        item.coluna,
        item.valor_antigo,
        item.valor_novo
    FROM (
        SELECT
            CAST(delta.objectid AS STRING) AS objectid_origem,
            [
                STRUCT('cartao_entregue' AS coluna, CAST(raw.cartao_entregue AS STRING) AS valor_antigo, CAST(delta.cartao_entregue AS STRING) AS valor_novo),
                STRUCT('local_entrega' AS coluna, CAST(raw.local_entrega AS STRING) AS valor_antigo, CAST(delta.local_entrega AS STRING) AS valor_novo),
                STRUCT('data_entrega_text' AS coluna, CAST(raw.data_entrega_text AS STRING) AS valor_antigo, CAST(delta.data_entrega_text AS STRING) AS valor_novo),
                STRUCT('resp_retirada' AS coluna, CAST(raw.resp_retirada AS STRING) AS valor_antigo, CAST(delta.resp_retirada AS STRING) AS valor_novo),
                STRUCT('data_particao_retirada' AS coluna, CAST(NULL AS STRING) AS valor_antigo, CAST(delta.data_particao_retirada AS STRING) AS valor_novo)
            ] AS updates
        FROM {{ ref('delta_feedback_controle') }} AS delta
        INNER JOIN {{ source('arcgis_raw', 'controle_cas_raw') }} AS raw ON delta.objectid = raw.objectid
    )
    CROSS JOIN UNNEST(updates) AS item
    WHERE COALESCE(item.valor_antigo, '') != COALESCE(item.valor_novo, '')
),

unificado AS (
    SELECT * FROM delta_pic
    UNION ALL
    SELECT * FROM delta_controle_unpivot
)

SELECT
    u.*,
    CURRENT_DATETIME('America/Sao_Paulo') AS timestamp_execucao,
    FARM_FINGERPRINT(TO_JSON_STRING(u)) AS unique_id
FROM unificado AS u
