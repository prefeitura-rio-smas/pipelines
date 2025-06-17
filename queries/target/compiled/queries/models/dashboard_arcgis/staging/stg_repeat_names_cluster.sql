

WITH base AS (
  SELECT
    parentrowid,
    nome_usuario_norm AS nm,
    nome_mae_norm     AS mm
  FROM `rj-smas-dev`.`dashboard_arcgis_dev`.`stg_repeat_names`
),

-- 1) BLOQUEIO ─ só pelo SOUNDEX do nome da pessoa
blocked AS (
  SELECT *,
         SOUNDEX(nm) AS block_key
  FROM base
),

-- 2) GERAR pares dentro do bloco
pairs AS (
  SELECT
    a.parentrowid AS id_a,
    b.parentrowid AS id_b,

    EDIT_DISTANCE(a.nm, b.nm) AS d_nm,
    EDIT_DISTANCE(a.mm, b.mm) AS d_mm,

    SPLIT(a.nm, ' ') AS a_nm_tok,
    SPLIT(b.nm, ' ') AS b_nm_tok,
    SPLIT(a.mm, ' ') AS a_mm_tok,
    SPLIT(b.mm, ' ') AS b_mm_tok
  FROM blocked a
  JOIN blocked b
    ON a.block_key = b.block_key
   AND a.parentrowid < b.parentrowid          -- evita reflexivos
),

-- 3) MÉTRICA de contenção   inter / menor ≥ 0.80
scored AS (
  SELECT *,
    ARRAY_LENGTH(ARRAY(
        SELECT x FROM UNNEST(a_nm_tok) x
        INTERSECT DISTINCT
        SELECT y FROM UNNEST(b_nm_tok) y
    )) / LEAST(ARRAY_LENGTH(a_nm_tok), ARRAY_LENGTH(b_nm_tok))  AS cont_nm,

    ARRAY_LENGTH(ARRAY(
        SELECT x FROM UNNEST(a_mm_tok) x
        INTERSECT DISTINCT
        SELECT y FROM UNNEST(b_mm_tok) y
    )) / LEAST(ARRAY_LENGTH(a_mm_tok), ARRAY_LENGTH(b_mm_tok))  AS cont_mm
  FROM pairs
),

-- 4) DUPLICADOS: (dist ≤2 OR cont ≥0.8) em AMBAS as colunas
dupes AS (
  SELECT id_a, id_b
  FROM scored
  WHERE (d_nm <= 2 OR cont_nm >= 0.8)
    AND (d_mm <= 2 OR cont_mm >= 0.8)
),

-- 5) CLUSTER: componente conexa via menor parentrowid
graph AS (
  SELECT parentrowid, parentrowid       AS neighbour FROM base   -- singlet
  UNION ALL
  SELECT id_a, id_b FROM dupes
  UNION ALL
  SELECT id_b, id_a FROM dupes          -- torna o grafo não-direcionado
),

clustered AS (
  SELECT
    parentrowid,
    MIN(neighbour) OVER (PARTITION BY parentrowid) AS cluster_seed
  FROM graph
)

SELECT DISTINCT
  b.parentrowid,
  MIN(cluster_seed) OVER (PARTITION BY cluster_seed) AS cluster_id,
  COUNT(*)        OVER (PARTITION BY cluster_seed)   AS cluster_size,
  cluster_size > 1                                   AS is_duplicado
FROM base b
JOIN clustered USING (parentrowid);