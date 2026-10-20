{% macro normalizar_itens_sociais(itens, nao_mapeado) %}
    array(
        select distinct as struct
            item.codigo,
            coalesce(item.descricao, format('{{ nao_mapeado }}', item.codigo)) as descricao
        from unnest({{ itens }}) as item
        where upper(trim(item.codigo)) not in ('', 'N')
        order by codigo
    )
{% endmacro %}
