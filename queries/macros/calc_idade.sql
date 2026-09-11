{% macro calc_idade(data_nascimento, data_referencia=none) %}
    -- Idade aniversario-correta na data de referência (expressão SQL como texto).
    -- Sem data_referencia, usa current_date() (comportamento original).
    -- Sentinel 'fim_do_mes' resolve last_day do mês de referência (var competencia).
    -- Ex. RMA: calc_idade('data_nascimento', 'fim_do_mes').
    {% if data_referencia is none %}
        {% set ref = "current_date()" %}
    {% elif data_referencia == 'fim_do_mes' %}
        {% set ref = "last_day(" ~ mes_referencia() ~ ")" %}
    {% else %}
        {% set ref = data_referencia %}
    {% endif %}
    date_diff({{ ref }}, {{ data_nascimento }}, year) -
    case
        when extract(month from {{ ref }}) < extract(month from {{ data_nascimento }})
            then 1
        when extract(month from {{ ref }}) = extract(month from {{ data_nascimento }})
              and extract(day from {{ ref }}) < extract(day from {{ data_nascimento }})
            then 1
        else 0
    end
{% endmacro %}
