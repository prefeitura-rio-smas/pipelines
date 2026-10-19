{% macro descricoes_sociais(itens) %}
    (select string_agg(distinct item.descricao, ', ' order by item.descricao)
     from unnest({{ itens }}) as item)
{% endmacro %}
