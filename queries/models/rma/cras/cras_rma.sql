{{ config(materialized='table') }}

WITH base_table AS (
SELECT 
gh_us.dscus,
a.total_familias_acomp_paif,
a.total_familias_acomp_paif_mes_atual,
b.total_familias_acomp_paif_bf,
b.famil_bf_cond_b3,
b.total_familias_acomp_paif_bpc,
b.famil_criancas_adoslecentes_trab_infantil_b5,
c.total_atendimentos_C1,
c.quantidade_encaminhamento_cadunico_C2C3,
c.quantidade_encaminhamento_bpc_C4,
c.quantidade_encaminhamento_creas_C5
FROM {{ source('brutos_acolherio_staging', 'gh_us') }} gh_us
LEFT JOIN {{ ref('bloco1_item_a_cras_v2')}} a ON gh_us.dscus = a.unidade
LEFT JOIN {{ ref('bloco1_item_b_cras_v2')}} b ON gh_us.dscus = b.unidade
LEFT JOIN {{ ref('bloco2_item_c_cras')}} c ON gh_us.dscus = c.dscus
)

SELECT * FROM base_table