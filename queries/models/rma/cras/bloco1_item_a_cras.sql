{{ config(materialized='table') }}

-- Total de indivíduos em acompanhamento PAIF no sistema (Item A1 do RMA - CRAS)
WITH a1 AS (
    SELECT
        dscus,
        COUNT(*) as total_paif_sistema
    FROM {{ ref('base_table_bloco1_item_a_cras')}}
    GROUP BY dscus
),

-- Select final contando por membro (Item A2 do RMA - CREAS)
a2 AS (
SELECT
    dscus,
    COUNT(*) as total_novos_paif_11
 FROM {{ ref('base_table_bloco1_item_a_cras')}}
 -- WHERE mes_cadastro = 11
 GROUP BY dscus
),

dscus_all AS (
    SELECT dscus FROM a1
    UNION DISTINCT
    SELECT dscus FROM a2
)

SELECT
    a.dscus,
    b.total_paif_sistema,
    c.total_novos_paif_11
FROM dscus_all a
LEFT JOIN a1 b ON a.dscus = b.dscus
LEFT JOIN a2 c ON a.dscus = c.dscus