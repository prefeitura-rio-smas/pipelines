{% macro fim_mes_referencia(nome_variavel='competencia') %}
    {{ fim_mes(mes_referencia(nome_variavel)) }}
{% endmacro %}
