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
    {%- if '%d' in nao_mapeado_fmt -%}
        {{ exceptions.raise_compiler_error(
            "acolherio_normalizar_itens: nao_mapeado_fmt não pode usar '%d' "
            ~ "(recebido: '" ~ nao_mapeado_fmt ~ "'). O FORMAT do BigQuery é inválido para STRING com '%d'; "
            ~ "use '%s', que também preserva zeros à esquerda do código."
        ) }}
    {%- endif -%}
    {%- set placeholders_s = nao_mapeado_fmt.count('%s') -%}
    {%- if placeholders_s != 1 -%}
        {{ exceptions.raise_compiler_error(
            "acolherio_normalizar_itens: nao_mapeado_fmt precisa conter exatamente um placeholder '%s' "
            ~ "(encontrados " ~ placeholders_s ~ " em: '" ~ nao_mapeado_fmt ~ "'). "
            ~ "O FORMAT do BigQuery é chamado com um único argumento (o código do item): "
            ~ "zero placeholders ou mais de um falham em runtime com 'Too few/many arguments to FORMAT'."
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
