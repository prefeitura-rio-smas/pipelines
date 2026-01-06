{{ config(materialized='ephemeral') }}

/*
Query para o bloco I item D2 
A opção violência intrafamiliar não existe no sistema ainda (Item D1)
*/

-- Tabela para resgatar o nome específico da violação de direito de cada membro da família.
WITH filtro_violacao_direito AS (
    SELECT
        a.seqfamil,
        a.seqmembro,
        a.seqpac,
        a.dscus,
        a.idade,
        b.violacao_direito
    FROM {{ ref('base_table_bloco1_item_c')}} a
    INNER JOIN {{ ref('violacao_direito')}} b ON a.seqpac = b.id_usuario
)

-- Query para buscar apenas idosos vitímas de negligência e abandono (ITEM D2 - RMA CREAS)
SELECT
    dscus,
    COUNT(*) AS idoso_negligencia_abandono
FROM filtro_violacao_direito
WHERE violacao_direito = 'Negligência e abandono'
AND idade > 60
GROUP BY dscus
