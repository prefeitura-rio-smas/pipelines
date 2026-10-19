{% macro inicio_mes(data_referencia) %}
    date_trunc(date({{ data_referencia }}), month)
{% endmacro %}
