{#
    Idade completa em qualquer expressão SQL de data de referência.
    Quando omitida, preserva o comportamento histórico de usar current_date().
#}
{% macro calc_idade(data_nascimento, data_referencia=none) %}
    {% set ref = data_referencia %}
    {% if ref is none %}
        {% set ref = 'current_date()' %}
    {% endif %}
    date_diff(date({{ ref }}), date({{ data_nascimento }}), year) -
    case
        when
            extract(month from date({{ ref }}))
            < extract(month from date({{ data_nascimento }}))
            then 1
        when
            extract(month from date({{ ref }}))
            = extract(month from date({{ data_nascimento }}))
            and extract(day from date({{ ref }}))
            < extract(day from date({{ data_nascimento }}))
            then 1
        else 0
    end
{% endmacro %}
