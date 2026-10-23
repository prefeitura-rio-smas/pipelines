{#
    Predicado de pertencimento ao mês. A referência pode ser qualquer expressão
    SQL de data; quando omitida, usa a competência configurada no projeto.
#}
{% macro no_mes(col_data, data_referencia=none) %}
    {% set referencia = data_referencia %}
    {% if referencia is none %}
        {% set referencia = mes_referencia() %}
    {% endif %}
    (
        date({{ col_data }}) >= {{ inicio_mes(referencia) }}
        and date({{ col_data }}) < date_add(
            {{ inicio_mes(referencia) }},
            interval 1 month
        )
    )
{% endmacro %}
