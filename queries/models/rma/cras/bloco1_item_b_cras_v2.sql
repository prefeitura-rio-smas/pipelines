{{ config(materialized='ephemeral') }}
-- Item B2 RMA CRAS
WITH familias_benef_bolsa_familia_novas_paif_mes_atual AS (
    SELECT 
        unidade, 
        COUNT(DISTINCT(seqfamil)) AS total_familias_acomp_paif_bf
    FROM {{ ref('base_filtro_bloco1_item_b_v2')}}
    WHERE beneficio = 'Bolsa Família'
    --AND mes_cadastro_assist = mes_atual
    GROUP BY unidade
),

-- Item B3 RMA CRAS
filtro_b3 AS (
    SELECT 
        a.seqfamil,
        a.id_usuario,
        a.unidade,
        a.data_nascimento,
        b.seqvulnerab
    FROM {{ ref('base_filtro_bloco1_item_b_v2')}} a
    INNER JOIN {{ source('brutos_acolherio_staging', 'gh_famil_vulnerab') }} b ON a.seqfamil = b.seqfamil
    WHERE a.beneficio = 'Bolsa Família'
),

b3 AS (
SELECT 
    unidade,
    COUNT(DISTINCT(seqfamil)) AS famil_bf_cond_b3
FROM filtro_b3
WHERE seqvulnerab = 1
GROUP BY unidade
),

-- Item B4 RMA CRAS
familias_benef_bpc_novas_paif_mes_atual AS (
    SELECT 
        unidade, 
        COUNT(DISTINCT(seqfamil)) AS total_familias_acomp_paif_bpc
    FROM {{ ref('base_filtro_bloco1_item_b_v2')}}
    WHERE beneficio = 'BPC-Benefício de Prestação Continuada'
    --AND mes_cadastro_assist = mes_atual
    GROUP BY unidade
),


filtro_violacao_direito_B5 AS (
    SELECT
        a.seqfamil,
        a.seqmembro,
        a.id_usuario,
        a.unidade,
        a.idade,
        b.violacao_direito
    FROM {{ ref('base_filtro_bloco1_item_b_v2')}} a
    INNER JOIN {{ ref('violacao_direito')}} b ON a.id_usuario = b.id_usuario
),

-- Query para buscar famílias com crinças ou adoslecentes em trabalho infantil (ITEM B5 - RMA CRAS)
b5 AS (
    SELECT
        unidade,
        COUNT(DISTINCT(seqfamil)) AS famil_criancas_adoslecentes_trab_infantil_b5
    FROM filtro_violacao_direito_B5
    WHERE violacao_direito = 'Trabalho Infantil'
    AND idade < 18
    GROUP BY unidade
),

dscus_all AS (
    SELECT unidade FROM familias_benef_bolsa_familia_novas_paif_mes_atual
    UNION DISTINCT
    SELECT unidade FROM familias_benef_bpc_novas_paif_mes_atual
    UNION DISTINCT
    SELECT unidade FROM b3
    UNION DISTINCT
    SELECT unidade FROM b5
)

SELECT
    a.unidade,
    b.total_familias_acomp_paif_bf,
    c.total_familias_acomp_paif_bpc,
    d.famil_bf_cond_b3,
    e.famil_criancas_adoslecentes_trab_infantil_b5
FROM dscus_all a
LEFT JOIN familias_benef_bolsa_familia_novas_paif_mes_atual b ON a.unidade = b.unidade
LEFT JOIN familias_benef_bpc_novas_paif_mes_atual c ON a.unidade = c.unidade
LEFT JOIN b3 d ON a.unidade = d.unidade
LEFT JOIN b5 e ON a.unidade = e.unidade