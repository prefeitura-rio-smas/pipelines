{% macro extrair_campos_html_evolucao(
    source_relation,
    id_cols,
    col_html = 'descricao_evolucao',
    extra_where = '',
    table_alias = 'src'
) %}
(
    -- Cada cabeçalho delimita uma seção; observações pertencem ao formulário anterior.
    -- Extrai campos da seção inteira: a fonte também usa <p><h3>...</h3>campos</p>.
    select
        {{ id_cols | join(', ') }},
        titulo_formulario,
        trim(regexp_replace(
            replace(regexp_extract(field, r'(?is)^(.*?)<b\b[^>]*>'), '&nbsp;', ' '),
            r':\s*$', ''
        )) as label,
        -- Quebras e limites de blocos separam palavras; tags inline só formatam.
        trim(replace(replace(regexp_replace(
            regexp_replace(
                regexp_extract(field, r'(?is)<b\b[^>]*>(.*?)</b\s*>'),
                r'(?is)</?(?:br|p|div|li)\b[^>]*>', ' '
            ), r'<[^>]*>', ''
        ), '&nbsp;', ' '), '&amp;', '&')) as valor,
        observacoes
    from (
        select
            {{ id_cols | join(', ') }},
            trim(regexp_replace(
                regexp_extract(secao, r'(?is)^(.*?)</h3\s*>'), r'<[^>]*>', ''
            )) as titulo_formulario,
            regexp_replace(secao, r'(?is)^.*?</h3\s*>', '') as bloco_campos,
            case
                when upper(trim(regexp_replace(
                    regexp_extract(
                        secoes[safe_offset(posicao + 1)], r'(?is)^(.*?)</h3\s*>'
                    ), r'<[^>]*>', ''
                ))) in ('OBSERVAÇÕES', 'CONTEÚDO E DESCRIÇÃO')
                    then regexp_replace(
                        secoes[safe_offset(posicao + 1)], r'(?is)^.*?</h3\s*>', ''
                    )
            end as observacoes
        from (
            select
                {{ id_cols | join(', ') }},
                split(regexp_replace(
                    {{ table_alias }}.{{ col_html }}, r'(?is)<h3\b[^>]*>', '\u001e'
                ), '\u001e') as secoes
            from {{ source_relation }} as {{ table_alias }}
            where regexp_contains({{ table_alias }}.{{ col_html }}, r'(?i)<b\b[^>]*>')
            {% if extra_where %}
              and {{ extra_where }}
            {% endif %}
        ) as evolucoes,
        unnest(secoes) as secao with offset as posicao
        where posicao > 0
    ) as formularios,
    unnest(regexp_extract_all(
        -- Remove formatação inline dos rótulos, preservando <b> e os limites dos campos.
        regexp_replace(
            bloco_campos, r'(?is)</?(?:span|em|i|u|font|small|s|strike|mark|a)\b[^>]*>', ''
        ), r'(?is)([^<>]+?:?\s*<b\b[^>]*>.*?</b\s*>)'
    )) as field
    where upper(titulo_formulario) not in ('OBSERVAÇÕES', 'CONTEÚDO E DESCRIÇÃO')
)
{% endmacro %}
