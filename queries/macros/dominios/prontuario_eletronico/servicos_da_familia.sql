{% macro servicos_da_familia_na_unidade(servicos, id_unidade) %}
    array(
        select as struct
            coalesce(s.nome_unidade, '') as nome_unidade,
            coalesce(cast(s.nome_servico as string), '') as nome_servico,
            coalesce(cast(s.data_cadastro as string), '') as data_cadastro,
            coalesce(cast(s.data_cancelamento as string), '') as data_cancelamento
        from unnest({{ servicos }}) as s with offset as posicao
        where s.id_unidade = {{ id_unidade }}
        order by posicao
    )
{% endmacro %}
