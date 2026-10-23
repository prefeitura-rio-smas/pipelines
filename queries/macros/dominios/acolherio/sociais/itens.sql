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
    {%- if '%s' not in nao_mapeado_fmt -%}
        {{ exceptions.raise_compiler_error(
            "acolherio_normalizar_itens: nao_mapeado_fmt precisa conter o placeholder '%s' "
            ~ "(recebido: '" ~ nao_mapeado_fmt ~ "'). O código é STRING; use %s, não %d."
        ) }}
    {%- endif -%}
    {%- if '%d' in nao_mapeado_fmt -%}
        {{ exceptions.raise_compiler_error(
            "acolherio_normalizar_itens: nao_mapeado_fmt não pode usar '%d' "
            ~ "(recebido: '" ~ nao_mapeado_fmt ~ "'). Use '%s': o código é string e zeros à esquerda devem ser preservados."
        ) }}
    {%- endif -%}
    {%- set fmt_literal = "'" ~ (nao_mapeado_fmt | replace("'", "''")) ~ "'" -%}
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
    {%- set sep_literal = "'" ~ (separador | replace("'", "''")) ~ "'" -%}
    (select string_agg(distinct item.{{ campo }}, {{ sep_literal }} order by item.{{ campo }})
     from unnest({{ itens }}) as item)
{%- endmacro %}
