{{ config(
    materialized='incremental',
    partition_by={
      "field": "data_particao",
      "data_type": "date",
      "granularity": "day"
    },
    incremental_strategy='insert_overwrite'
) }}

WITH parsed AS (
    SELECT * 
    FROM {{ ref('int_bolsa_familia_parsed') }}
    {% if is_incremental() %}
      -- No modo incremental, o dbt build processará apenas as partições que acabaram de ser carregadas no staging
      -- O dbt_utils ou uma macro customizada poderia ser usada aqui, mas usaremos a data_particao
      WHERE data_particao IN (SELECT DISTINCT data_particao FROM {{ source('bolsa_familia_staging', 'folha') }})
    {% endif %}
),

with_family_sum AS (
    SELECT 
        *,
        SUM(VLRBENEF) OVER (PARTITION BY data_particao, COD_FAMILIAR) AS beneficio_familiar_valor
    FROM parsed
)

SELECT 
    *,
    CASE 
        WHEN beneficio_familiar_valor <= 374 THEN 'ate 374'
        WHEN beneficio_familiar_valor <= 600 THEN '375 a 600'
        WHEN beneficio_familiar_valor <= 750 THEN '601 a 750'
        WHEN beneficio_familiar_valor <= 1350 THEN '751 a 1350'
        ELSE 'acima de 1350' 
    END AS beneficio_familiar_faixa
FROM with_family_sum
