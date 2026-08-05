{% macro extrair_ultima_atualizacao(tabela) %}
    (select max(data_extracao_origem) from {{ ref(tabela) }})
{% endmacro %}
