{% macro extrair_campos_html_evolucao(
    source_relation,
    id_cols,
    col_html = 'descricao_evolucao',
    table_alias = 'src'
) %}

WITH {{ table_alias }} AS (
    SELECT * FROM {{ source_relation }}
),

pares AS (
    SELECT
        {{ id_cols | join(', ') }},
        regexp_extract({{ table_alias }}.{{ col_html }}, r'<h3>(.*?)</h3>') as titulo_formulario,
        ARRAY(
            SELECT AS STRUCT
                regexp_extract(pair, r'^([^:]+?):\s*<b>[^<]*</b>') as label,
                regexp_extract(pair, r':\s*<b>([^<]*)</b>') as valor
            FROM UNNEST(
                regexp_extract_all({{ table_alias }}.{{ col_html }}, r'([^:<>]+?:\s*<b>[^<]*</b>)')
            ) AS pair
        ) as pares_array,
        regexp_extract(
            {{ table_alias }}.{{ col_html }},
            r'(?:OBSERVAÇÕES|CONTEÚDO E DESCRIÇÃO)\s*:?\s*([\s\S]*?)$'
        ) as observacoes
    FROM {{ table_alias }}
    WHERE {{ table_alias }}.{{ col_html }} LIKE '%<b>%'
),

evolucao_campos_extraidos AS (
    SELECT
        {{ id_cols | join(', ') }},
        titulo_formulario,
        p.label,
        p.valor,
        observacoes
    FROM pares,
    UNNEST(pares.pares_array) AS p
)

SELECT * FROM evolucao_campos_extraidos

{% endmacro %}
