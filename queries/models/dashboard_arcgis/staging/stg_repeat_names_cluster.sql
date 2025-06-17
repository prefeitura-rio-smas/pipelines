{{ config(materialized='view') }}

WITH base AS (
  SELECT
    parentrowid,
    nome_usuario_norm AS nm,
    nome_mae_norm     AS mm
  FROM {{ ref('stg_repeat_names') }}
),

-- 1) BLOQUEIO por SOUNDEX (evita pair-wise N²)
blocked AS (
  SELECT *,
         SOUNDEX(nm) AS sx_nm,
         SOUNDEX(mm) AS sx_mm
  FROM base
),

-- 2) GERAR pares só dentro do bloco
pairs AS (
  SELECT
    a.parentrowid AS id_a,
    b.parentrowid AS id_b,

    EDIT_DISTANCE(a.nm, b.nm) AS d_nm,
    EDIT_DISTANCE(a.mm, b.mm) AS d_mm,

    -- tokens
    SPLIT(a.nm, ' ') AS arr_nm_a,
    SPLIT(b.nm, ' ') AS arr_nm_b,
    SPLIT(a.mm, ' ') AS arr_mm_a,
    SPLIT(b.mm, ' ') AS arr_mm_b
  FROM blocked a
  JOIN blocked b
    ON a.sx_nm = b.sx_nm AND a.sx_mm = b.sx_mm
   AND a.parentrowid < b.parentrowid             -- evita espelho
),

-- 3) MÉTRICAS de contenção (tokens_inter / tokens_menor)
scored AS (
  SELECT *,
    ARRAY_LENGTH(
      (SELECT ARRAY_AGG(x) FROM UNNEST(arr_nm_a) x
                             INNER JOIN UNNEST(arr_nm_b) y ON x=y)
    ) / LEAST(ARRAY_LENGTH(arr_nm_a), ARRAY_LENGTH(arr_nm_b))         AS cont_nm,

    ARRAY_LENGTH(
      (SELECT ARRAY_AGG(x) FROM UNNEST(arr_mm_a) x
                             INNER JOIN UNNEST(arr_mm_b) y ON x=y)
    ) / LEAST(ARRAY_LENGTH(arr_mm_a), ARRAY_LENGTH(arr_mm_b))         AS cont_mm
  FROM pairs
),

-- 4) REGRA DE MATCH
dupes AS (
  SELECT id_a, id_b
  FROM scored
  WHERE
        (d_nm <= 2 OR cont_nm >= 0.8)
    AND (d_mm <= 2 OR cont_mm >= 0.8)
),

-- 5) CLUSTER  ➜ menor parentrowid do grupo
cluster_map AS (
  SELECT id_a AS parentrowid, LEAST(id_a, MIN(id_b)) AS seed
  FROM dupes
  GROUP BY id_a

  UNION ALL

  SELECT parentrowid, parentrowid
  FROM base                                   -- singletons
)

SELECT
  b.*,
  MIN(seed) OVER (PARTITION BY seed) AS cluster_id,
  COUNT(*)  OVER (PARTITION BY seed) AS cluster_size
FROM base b
LEFT JOIN cluster_map USING (parentrowid)
