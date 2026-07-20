{% macro extrair_campos_html_evolucao(
    source_relation,
    id_cols,
    col_html = 'descricao_evolucao',
    extra_where = '',
    table_alias = 'src'
) %}

WITH {{ table_alias }} AS (
    SELECT * FROM {{ source_relation }}
),

campos_extraidos AS (
    SELECT
        {{ id_cols | join(', ') }},
        REGEXP_EXTRACT({{ table_alias }}.{{ col_html }}, r'<p>(.*?)</p>') AS bloco_campos,
        REGEXP_EXTRACT({{ table_alias }}.{{ col_html }}, r'</p>\s*<h3>(?:OBSERVAÇÕES|CONTEÚDO E DESCRIÇÃO)</h3>\s*([\s\S]*?)$') AS observacoes,
        REGEXP_EXTRACT({{ table_alias }}.{{ col_html }}, r'<h3>(.*?)</h3>') AS titulo_formulario
    FROM {{ table_alias }}
    WHERE {{ table_alias }}.{{ col_html }} LIKE '%<b>%'
    {% if extra_where %}
      AND {{ extra_where }}
    {% endif %}
),

fields AS (
    SELECT
        {{ id_cols | join(', ') }},
        titulo_formulario,
        field,
        observacoes
    FROM campos_extraidos,
    UNNEST(REGEXP_EXTRACT_ALL(bloco_campos, r'[^<]+?:?\s*<b>[^<]*</b>')) AS field
    WHERE bloco_campos IS NOT NULL
),

evolucao_campos_extraidos AS (
    SELECT
        {{ id_cols | join(', ') }},
        titulo_formulario,
        TRIM(REGEXP_REPLACE(
            SPLIT(field, ': <b>')[SAFE_OFFSET(0)],
            r'^.*?>', ''
        )) AS label,
        TRIM(REGEXP_EXTRACT(field, r'<b>([^<]*)</b>')) AS valor,
        observacoes
    FROM fields
)

SELECT * FROM evolucao_campos_extraidos

{% endmacro %}
