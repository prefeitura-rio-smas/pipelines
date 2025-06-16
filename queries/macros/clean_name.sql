{% macro clean_name(col) %}
    TRIM(                                                         
      REGEXP_REPLACE(
        REGEXP_REPLACE(
          LOWER(
            REGEXP_REPLACE(NORMALIZE({{ col }}, NFD), r'\p{M}', '')  -- remove acentos
          ),
          r'[^a-z\s]', ' '        -- pontuação → espaço
        ),
        r'\s+', ' '               -- espaços duplicados
      )
    )
{% endmacro %}
