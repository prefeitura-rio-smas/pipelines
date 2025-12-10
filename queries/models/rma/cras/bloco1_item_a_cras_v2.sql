{{ config(materialized='table') }}
-- Item A1
WITH total_acompanhamento_paif_por_unidade AS (
    SELECT 
        unidade, 
        COUNT(seqfamil) AS total_familias_acomp_paif
    FROM {{ ref('base_filtro_contas_uma_unidade')}} 
    GROUP BY unidade
),

-- Query para Item A2
filtro_mes_atual AS(
    SELECT 
        *,
        EXTRACT(MONTH FROM datcadastr) AS mes_cadastro_assist,
        EXTRACT(MONTH FROM CURRENT_DATE()) AS mes_atual
    FROM {{ ref('base_filtro_contas_uma_unidade')}}
),
-- Item A2
total_acompanhamento_paif_novos_mes_atual AS (
    SELECT
        unidade,
        COUNT(seqfamil) AS total_familias_acomp_paif_mes_atual
    FROM filtro_mes_atual
    WHERE mes_cadastro_assist = mes_atual
    GROUP BY unidade
),

dscus_all AS (
    SELECT unidade FROM total_acompanhamento_paif_por_unidade
    UNION DISTINCT
    SELECT unidade FROM total_acompanhamento_paif_novos_mes_atual
)

SELECT
    a.unidade,
    b.total_familias_acomp_paif,
    c.total_familias_acomp_paif_mes_atual
FROM dscus_all a
LEFT JOIN total_acompanhamento_paif_por_unidade b ON a.unidade = b.unidade
LEFT JOIN total_acompanhamento_paif_novos_mes_atual c ON a.unidade = c.unidade