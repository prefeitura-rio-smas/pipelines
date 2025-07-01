{%- macro tratamento_duplicatas_cluster(
        source_relation,
        col_globalid       = 'globalid',
        col_id_principal   = 'parentrowid',
        col_usuario_raw    = 'repeat_nome_usuario',
        col_mae_raw        = 'repeat_nome_mae',
        pass_through_cols  = [],
        stop_words         = none,
        row_cap_blk        = 2000,
        edit_max           = 2,      
        cont_min           = 0.8     
) -%}

{%- set ids_all = [col_globalid, col_id_principal] + pass_through_cols -%}
{#- CORREÇÃO: A lista de colunas para o SELECT final. O join inclui o alias da tabela. -#}
{%- set pass_cols_select_sql = 'b.' ~ pass_through_cols | join(', b.') if pass_through_cols -%}
{%- set pass_cols_final_sql = pass_through_cols | join(', ') if pass_through_cols -%}


-- 1) padroniza nomes -----------------------------------------------------
WITH base_padronizada AS (
    {{ padronizacao_nomes(
         source_relation    = source_relation,
         id_cols            = ids_all,
         col_usuario        = col_usuario_raw,
         col_mae            = col_mae_raw,
         stop_words         = stop_words,
         keep_raw_columns   = false
    ) }}
),

-- 1. Base com nomes normalizados
base AS (
    SELECT
        {{ col_globalid }}   AS globalid,
        {{ col_id_principal }} AS id_principal,
        nome_usuario_norm,
        nome_mae_norm
        {%- if pass_through_cols %}, {{ pass_cols_final_sql }}{% endif %}
    FROM base_padronizada
),

-- 2. Bloqueio A  (SOUNDEX do nome)  ·········································
blk_a AS (
    SELECT
        *,
        SOUNDEX(nome_usuario_norm) AS blk
    FROM base
    QUALIFY ROW_NUMBER() OVER (PARTITION BY blk ORDER BY id_principal) <= 2000
),

pairs_a AS (
    SELECT
        a.id_principal AS id_a,
        b.id_principal AS id_b,
        a.nome_usuario_norm, a.nome_mae_norm,
        b.nome_usuario_norm AS nome_usuario_norm_b, b.nome_mae_norm AS nome_mae_norm_b
    FROM blk_a a
    JOIN blk_a b USING (blk)
    WHERE a.id_principal < b.id_principal
),

-- 3. Bloqueio B  (SOUNDEX do nome + último sobrenome da mãe) ···············
blk_b AS (
    SELECT
        *,
        CONCAT(
          SOUNDEX(nome_usuario_norm), '_',
          SOUNDEX(SPLIT(nome_mae_norm, ' ')[SAFE_OFFSET(-1)])  -- último token
        ) AS blk
    FROM base
    QUALIFY ROW_NUMBER() OVER (PARTITION BY blk ORDER BY id_principal) <= 2000
),

pairs_b AS (
    SELECT
        a.id_principal AS id_a,
        b.id_principal AS id_b,
        a.nome_usuario_norm, a.nome_mae_norm,
        b.nome_usuario_norm AS nome_usuario_norm_b, b.nome_mae_norm AS nome_mae_norm_b
    FROM blk_b a
    JOIN blk_b b USING (blk)
    WHERE a.id_principal < b.id_principal
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
        EDIT_DISTANCE(nome_usuario_norm,  nome_usuario_norm_b) AS d_nome_usuario_norm,
        EDIT_DISTANCE(nome_mae_norm,  nome_mae_norm_b) AS d_nome_mae_norm,

        ARRAY_LENGTH(
          ARRAY(
            SELECT x FROM UNNEST(SPLIT(nome_usuario_norm, ' ')) x
            INTERSECT DISTINCT
            SELECT y FROM UNNEST(SPLIT(nome_usuario_norm_b,' ')) y
          )
        ) / LEAST(
              ARRAY_LENGTH(SPLIT(nome_usuario_norm,  ' ')),
              ARRAY_LENGTH(SPLIT(nome_usuario_norm_b,' '))
          )                                   AS cont_nome_usuario_norm,

        ARRAY_LENGTH(
          ARRAY(
            SELECT x FROM UNNEST(SPLIT(nome_mae_norm, ' ')) x
            INTERSECT DISTINCT
            SELECT y FROM UNNEST(SPLIT(nome_mae_norm_b,' ')) y
          )
        ) / LEAST(
              ARRAY_LENGTH(SPLIT(nome_mae_norm,  ' ')),
              ARRAY_LENGTH(SPLIT(nome_mae_norm_b,' '))
          )                                   AS cont_nome_mae_norm
    FROM pairs
),

-- 6. Pares que satisfazem a regra de duplicidade ····························
dupes AS (
    SELECT id_a, id_b
    FROM scored
    WHERE (d_nome_usuario_norm <= 2 OR cont_nome_usuario_norm >= 0.8)
      AND (d_nome_mae_norm <= 2 OR cont_nome_mae_norm >= 0.8)
),

-- 7) Grafo não-direcionado (componentes conexas) ----------------------------------------------------
graph AS (
    SELECT id_principal AS node, id_principal AS neigh FROM base
    UNION ALL
    SELECT id_a, id_b FROM dupes
    UNION ALL
    SELECT id_b, id_a FROM dupes
),

seed AS (
    SELECT node AS id_principal, MIN(neigh) AS cluster_seed
    FROM graph
    GROUP BY node
),

-- 8. Resultado final (uma linha por id_principal) ····························
final AS (
    SELECT
        b.globalid,
        b.id_principal,
        b.nome_usuario_norm,
        b.nome_mae_norm,
        {%- if pass_through_cols %}
        {{ pass_cols_select_sql }},
        {%- endif %}
        MIN(cluster_seed) OVER (PARTITION BY cluster_seed) AS cluster_id,
        COUNT(*)        OVER (PARTITION BY cluster_seed)   AS cluster_size
    FROM base b
    JOIN seed USING (id_principal)
)

SELECT
    globalid,
    id_principal,
    nome_usuario_norm,
    nome_mae_norm,
    {%- if pass_through_cols %}
    {{ pass_cols_final_sql }},
    {%- endif %}
    cluster_id,
    cluster_size,
    cluster_size > 1 AS is_duplicado
FROM final

{%- endmacro -%}