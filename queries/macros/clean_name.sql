{% macro clean_name(col) %}
    -- 1) minúsculo + strip acentos
    REGEXP_REPLACE(
      REGEXP_REPLACE(
        LOWER(
          REGEXP_REPLACE(NORMALIZE({{ col }}, NFD), r'\p{M}', '')
        ),
        r'[^a-z\s]',            -- 2) remove pontuação
        ' '
      ),
      r'\s+', ' '              -- 3) espaços duplicados
    )
{% endmacro %}
