{% macro extrair_ultima_atualizacao(tabela) %}
    (select max(datetime(timestamp(data_extracao_origem), 'America/Sao_Paulo')) from {{ ref(tabela) }})
{% endmacro %}
