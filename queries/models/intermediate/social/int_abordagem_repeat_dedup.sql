{{ config(materialized = 'table') }}

-- Agrupamento de abordagens da mesma pessoa (clusters de duplicidade).
-- Grão: 1 linha por globalid (= 1 abordagem), com o cluster_id do registro
-- mestre e a flag is_duplicado. Grafo recursivo com 6 regras de match exato
-- dentro do mesmo ano (ver CTE Matches); substituiu a versão com SOUNDEX.
-- Consumido pela mart_abordagem (dashboard de Abordagem).

-- ==========================================================
-- Dedup via grafo recursivo com 6 regras de matching exato
-- Substitui o modelo anterior baseado em SOUNDEX
-- ==========================================================

WITH RECURSIVE

BASEDADOS AS (
    SELECT
        GLOBALID AS ID,
        KEY_CPF,
        KEY_NOME_USUARIO AS KEY_NM,
        KEY_NOME_MAE AS KEY_MAE,
        KEY_NOME_PAI AS KEY_PAI,
        REPEAT_DATA_NASCIMENTO,
        ANO_NUM_DATA_ABORDAGEM AS ANO
    FROM {{ ref('raw_abordagem_repeat') }}
),

MATCHES AS (
    -- REGRA 1: CPF
    SELECT
        ID AS ID_ORIGEM,
        MIN(ID) OVER (PARTITION BY KEY_CPF, ANO) AS ID_DESTINO
    FROM BASEDADOS
    WHERE KEY_CPF NOT IN ('99999999999', '00000000000', '11111111111') AND LENGTH(KEY_CPF) = 11 QUALIFY ID != ID_DESTINO

    UNION ALL

    -- REGRA 2: NOME USUARIO + NOME MAE
    SELECT
        ID AS ID_ORIGEM,
        MIN(ID) OVER (PARTITION BY KEY_NM, KEY_MAE, ANO) AS ID_DESTINO
    FROM BASEDADOS
    WHERE KEY_NM != '' AND KEY_MAE != '' QUALIFY ID != ID_DESTINO

    UNION ALL

    -- REGRA 3: NOME + CPF + DATA NASCIMENTO
    SELECT
        ID AS ID_ORIGEM,
        MIN(ID) OVER (PARTITION BY KEY_NM, KEY_CPF, REPEAT_DATA_NASCIMENTO, ANO) AS ID_DESTINO
    FROM BASEDADOS
    WHERE KEY_NM != '' AND LENGTH(KEY_CPF) = 11 AND REPEAT_DATA_NASCIMENTO IS NOT NULL QUALIFY ID != ID_DESTINO

    UNION ALL

    -- REGRA 4: NOME MAE + CPF + DATA NASCIMENTO
    SELECT
        ID AS ID_ORIGEM,
        MIN(ID) OVER (PARTITION BY KEY_MAE, KEY_CPF, REPEAT_DATA_NASCIMENTO, ANO) AS ID_DESTINO
    FROM BASEDADOS
    WHERE KEY_MAE != '' AND LENGTH(KEY_CPF) = 11 AND REPEAT_DATA_NASCIMENTO IS NOT NULL QUALIFY ID != ID_DESTINO

    UNION ALL

    -- REGRA 5: NOME MAE + NOME PAI + DATA NASCIMENTO
    SELECT
        ID AS ID_ORIGEM,
        MIN(ID) OVER (PARTITION BY KEY_MAE, KEY_PAI, REPEAT_DATA_NASCIMENTO, ANO) AS ID_DESTINO
    FROM BASEDADOS
    WHERE KEY_MAE != '' AND KEY_PAI != '' AND REPEAT_DATA_NASCIMENTO IS NOT NULL QUALIFY ID != ID_DESTINO

    UNION ALL

    -- REGRA 6: NOME MAE + NOME PAI + CPF
    SELECT
        ID AS ID_ORIGEM,
        MIN(ID) OVER (PARTITION BY KEY_MAE, KEY_PAI, KEY_CPF, ANO) AS ID_DESTINO
    FROM BASEDADOS
    WHERE KEY_MAE != '' AND KEY_PAI != '' AND LENGTH(KEY_CPF) = 11 QUALIFY ID != ID_DESTINO
),

GRAFOCONEXO AS (
    SELECT
        ID_ORIGEM,
        ID_DESTINO
    FROM MATCHES
    UNION ALL
    SELECT
        G.ID_ORIGEM,
        M.ID_DESTINO
    FROM GRAFOCONEXO AS G INNER JOIN MATCHES AS M ON G.ID_DESTINO = M.ID_ORIGEM
),

MAPA_DE_PARA AS (
    SELECT
        ID_ORIGEM AS ID_CLIENTE,
        MIN(ID_DESTINO) AS ID_MESTRE
    FROM GRAFOCONEXO
    GROUP BY ID_ORIGEM
)

SELECT
    ORIG.ID AS `globalid`,
    COALESCE(MAPA.ID_MESTRE, ORIG.ID) AS `cluster_id`,
    MAPA.ID_MESTRE IS NOT NULL AND MAPA.ID_MESTRE != ORIG.ID AS `is_duplicado`
FROM BASEDADOS AS ORIG
LEFT JOIN MAPA_DE_PARA AS MAPA ON ORIG.ID = MAPA.ID_CLIENTE
