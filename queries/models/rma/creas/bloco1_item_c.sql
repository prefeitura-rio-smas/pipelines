{{ config(materialized='ephemeral') }}

/*
Query para o bloco I itens C3, C4, e C5. 
Não foi encontrada a opção 'abuso sexual' em violações de direitos (Item C2)
A opção violência intrafamiliar não existe no sistema ainda (Iem C1)
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
),

-- Query para buscar apenas crianças ou adoslecentes vitímas de exploração sexual (ITEM C3 - RMA CREAS)
c3 AS (
    SELECT
        dscus,
        COUNT(*) AS criancas_adoslecentes_expl_sexual
    FROM filtro_violacao_direito
    WHERE violacao_direito = 'Exploração Sexual'
    AND idade < 18
    GROUP BY dscus
),

-- Query para buscar apenas crianças ou adoslecentes vitímas de negligência e abandono (ITEM C4 - RMA CREAS)
c4 AS (
    SELECT
        dscus,
        COUNT(*) AS criancas_adoslecentes_negligencia_abandono
    FROM filtro_violacao_direito
    WHERE violacao_direito = 'Negligência e abandono'
    AND idade < 18
    GROUP BY dscus
),

-- Query para buscar apenas crianças ou adoslecentes vitímas de trabalho infantil (ITEM C5 - RMA CREAS)
c5 AS (
    SELECT
        dscus,
        COUNT(*) AS criancas_adoslecentes_trab_infantil
    FROM filtro_violacao_direito
    WHERE violacao_direito = 'Trabalho Infantil'
    AND idade <= 15
    GROUP BY dscus
),

dscus_all AS (
    SELECT dscus FROM c3
    UNION DISTINCT
    SELECT dscus FROM c4
    UNION DISTINCT 
    SELECT dscus FROM c5
)

SELECT
    a.dscus,
    b.criancas_adoslecentes_expl_sexual,
    c.criancas_adoslecentes_negligencia_abandono,
    d.criancas_adoslecentes_trab_infantil
FROM dscus_all a
LEFT JOIN c3 b ON a.dscus = b.dscus
LEFT JOIN c4 c ON a.dscus = c.dscus
LEFT JOIN c5 d ON a.dscus = d.dscus