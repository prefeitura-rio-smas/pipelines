{{ config(materialized='ephemeral') }}

/*
Query para o bloco I item H1
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

-- Query para buscar pessoas vitímas de discriminação por orientação sexual (ITEM H1 - RMA CREAS)
SELECT
    seqpac,
    dscus,
    COUNT(*) AS discriminacao_sexual
FROM filtro_violacao_direito
WHERE violacao_direito = 'Discriminação Sexual / Gênero'
GROUP BY dscus, seqpac