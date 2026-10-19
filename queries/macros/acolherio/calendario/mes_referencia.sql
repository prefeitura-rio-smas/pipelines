{% macro mes_referencia(nome_variavel='competencia') %}
    {% set competencia = var(nome_variavel, '') %}
    {% if competencia is none or competencia | string | trim == '' %}
        {{ inicio_mes('current_date()') }}
    {% else %}
        date('{{ competencia }}-01')
    {% endif %}
{% endmacro %}
