{% macro extrair_ultima_atualizacao(tabela) %}
    (select max(timestamp(data_extracao_origem, 'America/Sao_Paulo')) from {{ ref(tabela) }})
{% endmacro %}
