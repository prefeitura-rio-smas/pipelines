{{ config(materialized='table') }}

WITH base_table AS (
SELECT 
gh_us.dscus,
a.total_paefi_sistema,
a.total_novos_paefi_11,
b.quantidade_familia_bolsa_familia,
b.quantidade_familia_bpc,
b.familia_criancas_adoslecentes_trab_infantil,
b.pessoas_vitimas_violacao,
c.criancas_adoslecentes_expl_sexual,
c.criancas_adoslecentes_negligencia_abandono,
c.criancas_adoslecentes_trab_infantil,
d.idoso_negligencia_abandono,
e.deficiencia_negligencia_abandono,
g.vitima_trafico_ser_humano,
h.discriminacao_sexual,
i.situacao_de_rua
FROM {{ source('brutos_acolherio_staging', 'gh_us') }} gh_us
LEFT JOIN {{ ref('bloco1_item_a')}} a ON gh_us.dscus = a.dscus
LEFT JOIN {{ ref('bloco1_item_b')}} b ON gh_us.dscus = b.dscus
LEFT JOIN {{ ref('bloco1_item_c')}} c ON gh_us.dscus = c.dscus
LEFT JOIN {{ ref('bloco1_item_d')}} d ON gh_us.dscus = d.dscus
LEFT JOIN {{ ref('bloco1_item_e')}} e ON gh_us.dscus = e.dscus
LEFT JOIN {{ ref('bloco1_item_g')}} g ON gh_us.dscus = g.dscus
LEFT JOIN {{ ref('bloco1_item_h')}} h ON gh_us.dscus = h.dscus
LEFT JOIN {{ ref('bloco1_item_i')}} i ON gh_us.dscus = i.dscus
)

SELECT * FROM base_table