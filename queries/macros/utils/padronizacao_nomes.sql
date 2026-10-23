{%- macro padronizacao_nomes(
        source_relation,          
        id_cols,                  
        col_usuario,              
        col_mae,                 
        stop_words = none,       
        table_alias = 'src',      
        keep_raw_columns = false  
) -%}

{%- set default_sw = ['de','da','do','das','dos','e','a','o','os','as'] -%}
{%- set sw = stop_words if stop_words is not none else default_sw -%}
{%- set sw_sql = "ARRAY<STRING>['" ~ (sw | join("', '")) ~ "']" -%}

WITH {{ table_alias }} AS (
    SELECT * FROM {{ source_relation }}
),

tokens AS (
    SELECT
        {{ id_cols | join(', ') }},
        {% if keep_raw_columns -%}
        {{ table_alias }}.{{ col_usuario }} AS {{ col_usuario }},
        {{ table_alias }}.{{ col_mae }}     AS {{ col_mae }},
        {%- endif %}
        SPLIT({{ clean_name(table_alias ~ '.' ~ col_usuario) }}, ' ') AS arr_usuario,
        SPLIT({{ clean_name(table_alias ~ '.' ~ col_mae) }},     ' ') AS arr_mae
    FROM {{ table_alias }}
),

filtered AS (
    SELECT
        *,
        ARRAY(
          SELECT w FROM UNNEST(arr_usuario) w
          WHERE w NOT IN UNNEST({{ sw_sql }})
        ) AS arr_usuario_ok,
        ARRAY(
          SELECT w FROM UNNEST(arr_mae) w
          WHERE w NOT IN UNNEST({{ sw_sql }})
        ) AS arr_mae_ok
    FROM tokens
)

SELECT
    {{ id_cols | join(', ') }},
    {% if keep_raw_columns -%}
    {{ col_usuario }},
    {{ col_mae }},
    {%- endif %}
    ARRAY_TO_STRING(arr_usuario_ok, ' ') AS nome_usuario_norm,
    ARRAY_TO_STRING(arr_mae_ok,     ' ') AS nome_mae_norm
FROM filtered

{%- endmacro -%}
