{% macro fim_mes(data_referencia) %}
    last_day(date({{ data_referencia }}), month)
{% endmacro %}
