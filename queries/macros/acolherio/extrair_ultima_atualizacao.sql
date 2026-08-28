{% macro extrair_ultima_atualizacao(tabela) %}
    (select max(timestamp(data_extracao_origem) at time zone 'America/Sao_Paulo') from {{ ref(tabela) }})
{% endmacro %}
