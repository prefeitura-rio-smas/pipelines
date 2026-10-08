{% macro extrair_campos_html_evolucao(
    source_relation,
    id_cols,
    col_html = 'descricao_evolucao',
    extra_where = '',
    table_alias = 'src'
) %}
(
    with evolucoes as (
        select
            {{ id_cols | join(', ') }},
            split(
                regexp_replace(
                    {{ table_alias }}.{{ col_html }},
                    r'(?is)<h3\b[^>]*>',
                    '\u001e'
                ),
                '\u001e'
            ) as secoes_html
        from {{ source_relation }} as {{ table_alias }}
        where regexp_contains(
            {{ table_alias }}.{{ col_html }},
            r'(?i)<b\b[^>]*>'
        )
        {% if extra_where %}
            and {{ extra_where }}
        {% endif %}
    ),

    secoes as (
        select
            {{ id_cols | join(', ') }},
            secao,
            posicao,
            secoes_html[safe_offset(posicao + 1)] as proxima_secao
        from evolucoes,
            unnest(secoes_html) as secao with offset as posicao
        where posicao > 0
    ),

    secoes_com_proxima as (
        select
            {{ id_cols | join(', ') }},
            secao,
            trim(regexp_replace(
                regexp_extract(proxima_secao, r'(?is)^(.*?)</h3\s*>'),
                r'<[^>]*>',
                ''
            )) as titulo_proxima_secao,
            regexp_replace(proxima_secao, r'(?is)^.*?</h3\s*>', '') as conteudo_proxima_secao
        from secoes
    ),

    formularios as (
        select
            {{ id_cols | join(', ') }},
            trim(regexp_replace(
                regexp_extract(secao, r'(?is)^(.*?)</h3\s*>'),
                r'<[^>]*>',
                ''
            )) as titulo_formulario,
            regexp_replace(secao, r'(?is)^.*?</h3\s*>', '') as bloco_campos,
            case
                when upper(titulo_proxima_secao) in ('OBSERVAÇÕES', 'CONTEÚDO E DESCRIÇÃO')
                    then conteudo_proxima_secao
            end as observacoes
        from secoes_com_proxima
    ),

    campos as (
        select
            {{ id_cols | join(', ') }},
            titulo_formulario,
            observacoes,
            field
        from formularios,
            unnest(regexp_extract_all(
                regexp_replace(
                    bloco_campos,
                    r'(?is)</?(?:span|em|i|u|font|small|s|strike|mark|a)\b[^>]*>',
                    ''
                ),
                r'(?is)([^<>]+?:?\s*<b\b[^>]*>.*?</b\s*>)'
            )) as field
        where upper(titulo_formulario) not in (
            'OBSERVAÇÕES',
            'CONTEÚDO E DESCRIÇÃO'
        )
    )

    select
        {{ id_cols | join(', ') }},
        titulo_formulario,
        trim(regexp_replace(
            replace(
                regexp_extract(field, r'(?is)^(.*?)<b\b[^>]*>'),
                '&nbsp;',
                ' '
            ),
            r':\s*$',
            ''
        )) as label,
        trim(replace(replace(regexp_replace(
            regexp_replace(
                regexp_extract(field, r'(?is)<b\b[^>]*>(.*?)</b\s*>'),
                r'(?is)</?(?:br|p|div|li)\b[^>]*>',
                ' '
            ),
            r'<[^>]*>',
            ''
        ), '&nbsp;', ' '), '&amp;', '&')) as valor,
        observacoes
    from campos
)
{% endmacro %}
