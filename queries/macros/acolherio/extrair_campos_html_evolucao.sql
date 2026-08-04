{% macro extrair_campos_html_evolucao(
    source_relation,
    id_cols,
    col_html = 'descricao_evolucao',
    extra_where = '',
    table_alias = 'src'
) %}
(
    -- Nested subqueries (no WITH, pois BigQuery nao permite WITH dentro de FROM)
    SELECT
        {{ id_cols | join(', ') }},
        titulo_formulario,
        TRIM(REGEXP_REPLACE(
            REGEXP_EXTRACT(field, r'^(.*?):?\s*<b>'),
            r'^.*?>', ''
        )) AS label,
        TRIM(REGEXP_EXTRACT(field, r'<b>([^<]*)</b>')) AS valor,
        observacoes
    FROM (
        SELECT
            {{ id_cols | join(', ') }},
            titulo_formulario,
            field,
            observacoes
        FROM (
            SELECT
                {{ id_cols | join(', ') }},
                REGEXP_EXTRACT({{ table_alias }}.{{ col_html }}, r'<p>(.*?)</p>') AS bloco_campos,
                REGEXP_EXTRACT({{ table_alias }}.{{ col_html }}, r'</p>\s*<h3>(?:OBSERVAÇÕES|CONTEÚDO E DESCRIÇÃO)</h3>\s*([\s\S]*?)$') AS observacoes,
                REGEXP_EXTRACT({{ table_alias }}.{{ col_html }}, r'<h3>(.*?)</h3>') AS titulo_formulario
            FROM {{ source_relation }} AS {{ table_alias }}
            WHERE {{ table_alias }}.{{ col_html }} LIKE '%<b>%'
            {% if extra_where %}
              AND {{ extra_where }}
            {% endif %}
        ),
        UNNEST(REGEXP_EXTRACT_ALL(bloco_campos, r'[^<]+?:?\s*<b>[^<]*</b>')) AS field
        WHERE bloco_campos IS NOT NULL
    )
)
{% endmacro %}
