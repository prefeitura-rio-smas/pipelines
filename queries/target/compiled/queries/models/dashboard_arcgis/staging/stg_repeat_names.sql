

WITH base AS (
  SELECT
    parentrowid,
    repeat_nome_usuario,
    repeat_nome_mae
  FROM `rj-smas-dev`.`arcgis_raw`.`abordagem_repeat_raw`
),

tokens AS (
  SELECT
    parentrowid,
    repeat_nome_usuario,
    repeat_nome_mae,

    -- explode em palavras já normalizadas
    SPLIT(
    -- 1) minúsculo + strip acentos
    REGEXP_REPLACE(
      REGEXP_REPLACE(
        LOWER(
          REGEXP_REPLACE(NORMALIZE(repeat_nome_usuario, NFD), r'\p{M}', '')
        ),
        r'[^a-z\s]',            -- 2) remove pontuação
        ' '
      ),
      r'\s+', ' '              -- 3) espaços duplicados
    )
, ' ')     AS arr_usuario,
    SPLIT(
    -- 1) minúsculo + strip acentos
    REGEXP_REPLACE(
      REGEXP_REPLACE(
        LOWER(
          REGEXP_REPLACE(NORMALIZE(repeat_nome_mae, NFD), r'\p{M}', '')
        ),
        r'[^a-z\s]',            -- 2) remove pontuação
        ' '
      ),
      r'\s+', ' '              -- 3) espaços duplicados
    )
,     ' ')     AS arr_mae
  FROM base
),

filtered AS (
  SELECT
    parentrowid,
    repeat_nome_usuario,
    repeat_nome_mae,

    -- remove stop-words simples
    ARRAY(
      SELECT t
      FROM UNNEST(arr_usuario) t
      WHERE t NOT IN ('de','da','do','das','dos','e','a','o','os','as')
    ) AS arr_usuario_ok,

    ARRAY(
      SELECT t
      FROM UNNEST(arr_mae) t
      WHERE t NOT IN ('de','da','do','das','dos','e','a','o','os','as')
    ) AS arr_mae_ok
  FROM tokens
)

SELECT
  parentrowid,
  repeat_nome_usuario,
  repeat_nome_mae,

  ARRAY_TO_STRING(arr_usuario_ok, ' ')  AS nome_usuario_norm,
  ARRAY_TO_STRING(arr_mae_ok,     ' ')  AS nome_mae_norm
FROM filtered