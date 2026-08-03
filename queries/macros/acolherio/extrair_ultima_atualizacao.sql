{% macro extrair_ultima_atualizacao(tabela) %}
    select max(data_extracao_origem) as ultima_atualizacao from {{ ref(tabela) }}
{% endmacro %}
