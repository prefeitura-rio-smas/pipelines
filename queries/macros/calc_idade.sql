{% macro calc_idade(data_nascimento, data_referencia=none) %}
    -- Idade aniversario-correta na data de referência (expressão SQL como texto).
    -- Sem data_referencia, usa current_date() (comportamento original).
    -- Ex. RMA: calc_idade('data_nascimento', 'last_day(' ~ mes_referencia() ~ ')').
    {% if data_referencia is none %}
        {% set ref = "current_date()" %}
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
