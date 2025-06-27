

WITH base AS (
  SELECT
    globalid,
    parentrowid,
    repeat_nome_usuario,
    repeat_nome_mae,
    ano_num_data_abordagem
  FROM `rj-smas-dev`.`arcgis_raw`.`abordagem_repeat_raw`
),

tokens AS (
  SELECT
    *,

    -- explode em palavras já normalizadas
    SPLIT(
    TRIM(                                                         
      REGEXP_REPLACE(
        REGEXP_REPLACE(
          LOWER(
            REGEXP_REPLACE(NORMALIZE(repeat_nome_usuario, NFD), r'\p{M}', '')  -- remove acentos
          ),
          r'[^a-z\s]', ' '        -- pontuação → espaço
        ),
        r'\s+', ' '               -- espaços duplicados
      )
    )
, ' ')     AS arr_usuario,
    SPLIT(
    TRIM(                                                         
      REGEXP_REPLACE(
        REGEXP_REPLACE(
          LOWER(
            REGEXP_REPLACE(NORMALIZE(repeat_nome_mae, NFD), r'\p{M}', '')  -- remove acentos
          ),
          r'[^a-z\s]', ' '        -- pontuação → espaço
        ),
        r'\s+', ' '               -- espaços duplicados
      )
    )
,     ' ')     AS arr_mae
  FROM base
),

filtered AS (
  SELECT
    *,

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
  globalid,
  parentrowid,
  repeat_nome_usuario,
  repeat_nome_mae,
  ano_num_data_abordagem,

  ARRAY_TO_STRING(arr_usuario_ok, ' ')  AS nome_usuario_norm,
  ARRAY_TO_STRING(arr_mae_ok,     ' ')  AS nome_mae_norm
FROM filtered