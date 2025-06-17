{{ config(materialized='view') }}

WITH base AS (
  SELECT
    parentrowid,
    repeat_nome_usuario,
    repeat_nome_mae,
    ano_num_data_abordagem
  FROM {{ source('arcgis_raw', 'abordagem_repeat_raw') }}
),

tokens AS (
  SELECT
    parentrowid,
    repeat_nome_usuario,
    repeat_nome_mae,
    ano_num_data_abordagem,

    -- explode em palavras já normalizadas
    SPLIT({{ clean_name('repeat_nome_usuario') }}, ' ')     AS arr_usuario,
    SPLIT({{ clean_name('repeat_nome_mae') }},     ' ')     AS arr_mae
  FROM base
),

filtered AS (
  SELECT
    parentrowid,
    repeat_nome_usuario,
    repeat_nome_mae,
    ano_num_data_abordagem,

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
  ano_num_data_abordagem,

  ARRAY_TO_STRING(arr_usuario_ok, ' ')  AS nome_usuario_norm,
  ARRAY_TO_STRING(arr_mae_ok,     ' ')  AS nome_mae_norm
FROM filtered
