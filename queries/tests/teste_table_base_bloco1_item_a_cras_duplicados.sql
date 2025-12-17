-- Teste para verificar se há registros de famílias duplicados.
SELECT
    seqfamil,
    dscus,
    COUNT(*) as qtd
FROM {{ ref('base_table_bloco1_item_a_cras')}}
WHERE dscus IS NOT NULL
GROUP BY seqfamil, dscus
HAVING count(*) > 1