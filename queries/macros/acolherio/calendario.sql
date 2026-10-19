-- Macros de calendario: nomes e regras preservados.

{% macro inicio_mes(data_referencia) %}
    date_trunc(date({{ data_referencia }}), month)
{% endmacro %}

{% macro fim_mes(data_referencia) %}
    last_day(date({{ data_referencia }}), month)
{% endmacro %}

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

{% macro nao_cancelado(coluna='flag_cancelado', valor_cancelado='S') %}
    {% if valor_cancelado is none %}
        {{ coluna }} is null
    {% else %}
        (
            {{ coluna }} is null
            or {{ coluna }} != '{{ valor_cancelado | replace("'", "''") }}'
        )
    {% endif %}
{% endmacro %}

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
