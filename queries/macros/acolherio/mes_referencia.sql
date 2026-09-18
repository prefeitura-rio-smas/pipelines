{# Primitivas de calendário para expressões SQL temporais. #}
{% macro inicio_mes(data_referencia) %}
    date_trunc(date({{ data_referencia }}), month)
{% endmacro %}

{% macro fim_mes(data_referencia) %}
    last_day(date({{ data_referencia }}), month)
{% endmacro %}

{#
    Primeiro dia da competência mensal configurada no dbt.
    nome_variavel permite reutilizar a macro em relatórios com outra var.
    A variável deve usar o formato AAAA-MM; ausente/vazia usa o mês corrente.
#}
{% macro mes_referencia(nome_variavel='competencia') %}
    {% set competencia = var(nome_variavel, '') %}
    {% if competencia is none or competencia | string | trim == '' %}
        {{ inicio_mes('current_date()') }}
    {% else %}
        date('{{ competencia }}-01')
    {% endif %}
{% endmacro %}

{% macro fim_mes_referencia(nome_variavel='competencia') %}
    {{ fim_mes(mes_referencia(nome_variavel)) }}
{% endmacro %}
