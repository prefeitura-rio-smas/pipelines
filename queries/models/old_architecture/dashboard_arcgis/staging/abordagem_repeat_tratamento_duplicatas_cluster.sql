{{ config(materialized = 'ephemeral') }}

-- 1. Base com nomes normalizados
WITH base AS (
    SELECT
        globalid,
        parentrowid,
        nome_usuario_norm AS nm,
        nome_mae_norm     AS mm,
        ano_num_data_abordagem
    FROM {{ ref('abordagem_repeat_padronizacao_nomes') }}
),

-- 2. Bloqueio A  (SOUNDEX do nome)  ·········································
blk_a AS (
    SELECT
        *,
        SOUNDEX(nm) AS blk
    FROM base
    QUALIFY ROW_NUMBER() OVER (PARTITION BY blk ORDER BY parentrowid) <= 2000
),

pairs_a AS (
    SELECT
        a.parentrowid AS id_a,
        b.parentrowid AS id_b,
        a.nm, a.mm,
        b.nm AS nm_b, b.mm AS mm_b
    FROM blk_a a
    JOIN blk_a b USING (blk)
    WHERE a.parentrowid < b.parentrowid
),

-- 3. Bloqueio B  (SOUNDEX do nome + último sobrenome da mãe) ···············
blk_b AS (
    SELECT
        *,
        CONCAT(
          SOUNDEX(nm), '_',
          SOUNDEX(SPLIT(mm, ' ')[SAFE_OFFSET(-1)])  -- último token
        ) AS blk
    FROM base
    QUALIFY ROW_NUMBER() OVER (PARTITION BY blk ORDER BY parentrowid) <= 2000
),

pairs_b AS (
    SELECT
        a.parentrowid AS id_a,
        b.parentrowid AS id_b,
        a.nm, a.mm,
        b.nm AS nm_b, b.mm AS mm_b
    FROM blk_b a
    JOIN blk_b b USING (blk)
    WHERE a.parentrowid < b.parentrowid
),

-- 4. União dos pares candidatos ·············································
pairs AS (
    SELECT * FROM pairs_a
    UNION ALL
    SELECT * FROM pairs_b
),

-- 5. Métricas de distância e contenção ······································
scored AS (
    SELECT
        id_a, id_b,
        EDIT_DISTANCE(nm,  nm_b) AS d_nm,
        EDIT_DISTANCE(mm,  mm_b) AS d_mm,

        ARRAY_LENGTH(
          ARRAY(
            SELECT x FROM UNNEST(SPLIT(nm, ' ')) x
            INTERSECT DISTINCT
            SELECT y FROM UNNEST(SPLIT(nm_b,' ')) y
          )
        ) / LEAST(
              ARRAY_LENGTH(SPLIT(nm,  ' ')),
              ARRAY_LENGTH(SPLIT(nm_b,' '))
          )                                   AS cont_nm,

        ARRAY_LENGTH(
          ARRAY(
            SELECT x FROM UNNEST(SPLIT(mm, ' ')) x
            INTERSECT DISTINCT
            SELECT y FROM UNNEST(SPLIT(mm_b,' ')) y
          )
        ) / LEAST(
              ARRAY_LENGTH(SPLIT(mm,  ' ')),
              ARRAY_LENGTH(SPLIT(mm_b,' '))
          )                                   AS cont_mm
    FROM pairs
),

-- 6. Pares que satisfazem a regra de duplicidade ····························
dupes AS (
    SELECT id_a, id_b
    FROM scored
    WHERE (d_nm <= 2 OR cont_nm >= 0.8)
      AND (d_mm <= 2 OR cont_mm >= 0.8)
),

-- 7. Grafo não-direcionado (componentes conexas) ····························
graph AS (
    SELECT parentrowid, parentrowid AS neigh FROM base
    UNION ALL
    SELECT id_a, id_b FROM dupes
    UNION ALL
    SELECT id_b, id_a FROM dupes
),

seed AS (
    SELECT
        parentrowid,
        MIN(neigh) AS cluster_seed          
    FROM graph
    GROUP BY parentrowid                   
),

-- 8. Resultado final (uma linha por parentrowid) ····························
final AS (
    SELECT
        b.globalid,
        b.parentrowid,
        b.nm,
        b.mm,
        b.ano_num_data_abordagem,
        MIN(cluster_seed) OVER (PARTITION BY cluster_seed) AS cluster_id,
        COUNT(*)        OVER (PARTITION BY cluster_seed)   AS cluster_size
    FROM base b
    JOIN seed USING (parentrowid)
)

SELECT
    globalid,
    parentrowid,
    nm,
    mm,
    ano_num_data_abordagem,
    cluster_id,
    cluster_size,
    cluster_size > 1 AS is_duplicado
FROM final
