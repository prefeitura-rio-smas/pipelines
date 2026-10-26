{#-
    Macros de itens sociais (código + descrição) do AcolheRio.
    Uso típico sobre arrays STRUCT(codigo, descricao) já agregados por usuário:

        {{ acolherio_normalizar_itens('beneficio') }}
        {{ acolherio_normalizar_itens('deficiencia', 'Deficiência não mapeada (código %s)') }}
        {{ acolherio_descricoes_itens('beneficios_detalhes') }}
        {{ acolherio_descricoes_itens('deficiencias_detalhes', separador='; ') }}
-#}

{% macro acolherio_normalizar_itens(
    itens,
    nao_mapeado_fmt='Item não mapeado (código %s)',
    campo_codigo='codigo',
    campo_descricao='descricao'
) %}
    {%- set formato_sem_literais = nao_mapeado_fmt | replace('%%', '') -%}
    {%- if formato_sem_literais.count('%s') != 1
        or '%' in (formato_sem_literais | replace('%s', '')) -%}
        {{ exceptions.raise_compiler_error(
            "acolherio_normalizar_itens: nao_mapeado_fmt precisa conter exatamente um placeholder '%s' "
            ~ "(recebido: '" ~ nao_mapeado_fmt ~ "'). Use '%%' para um percentual literal; "
            ~ "outros especificadores (como '%d') e percentuais soltos não são aceitos."
        ) }}
    {%- endif -%}
    {# GoogleSQL usa barra invertida; escape-a antes dos demais caracteres. #}
    {%- set fmt_literal = "'" ~ (nao_mapeado_fmt
        | replace('\\', '\\\\')
        | replace("'", "\\'")
        | replace('\n', '\\n')
        | replace('\r', '\\r')
        | replace('\t', '\\t')
        | replace('\b', '\\b')
        | replace('\f', '\\f')
        | replace('\a', '\\a')
        | replace('\v', '\\v')
    ) ~ "'" -%}
array(
        select distinct as struct
            upper(trim(cast(item.{{ campo_codigo }} as string))) as codigo,
            coalesce(
                item.{{ campo_descricao }},
                format({{ fmt_literal }}, upper(trim(cast(item.{{ campo_codigo }} as string))))
            ) as descricao
        from unnest({{ itens }}) as item
        where upper(trim(cast(item.{{ campo_codigo }} as string))) not in ('', 'N')
        order by codigo
    )
{%- endmacro %}

{% macro acolherio_descricoes_itens(itens, campo='descricao', separador=', ') %}
    {%- set sep_literal = "'" ~ (separador
        | replace('\\', '\\\\')
        | replace("'", "\\'")
        | replace('\n', '\\n')
        | replace('\r', '\\r')
        | replace('\t', '\\t')
        | replace('\b', '\\b')
        | replace('\f', '\\f')
        | replace('\a', '\\a')
        | replace('\v', '\\v')
    ) ~ "'" -%}
    (select string_agg(distinct item.{{ campo }}, {{ sep_literal }} order by item.{{ campo }})
     from unnest({{ itens }}) as item)
{%- endmacro %}
