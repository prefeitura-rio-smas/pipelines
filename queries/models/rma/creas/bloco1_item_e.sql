{{ config(materialized='ephemeral') }}

/*
Query para o bloco I item E2 
A opção violência intrafamiliar não existe no sistema ainda (Item E1)
*/

-- Tabela para resgatar o nome específico da violação de direito de cada membro da família.
WITH filtro_violacao_direito AS (
    SELECT
        a.seqfamil,
        a.seqmembro,
        a.seqpac,
        a.dscus,
        a.idade,
        a.deficiencia,
        b.violacao_direito
    FROM {{ ref('base_table_bloco1_item_c')}} a
    INNER JOIN {{ ref('violacao_direito')}} b ON a.seqpac = b.id_usuario
)

-- Query para buscar pessoas com deficiência vitímas de negligência e abandono (ITEM E2 - RMA CREAS)
SELECT
    seqpac,
    dscus,
    COUNT(*) AS deficiencia_negligencia_abandono
FROM filtro_violacao_direito
WHERE violacao_direito = 'Negligência e abandono'
AND deficiencia = 'S'
GROUP BY dscus, seqpac
