{% macro calc_idade(data_nascimento) %}
    date_diff(current_date(), {{ data_nascimento }}, year) -
    case
        when extract(month from current_date()) < extract(month from {{ data_nascimento }})
            then 1
        when extract(month from current_date()) = extract(month from {{ data_nascimento }})
             and extract(day from current_date()) < extract(day from {{ data_nascimento }})
            then 1
        else 0
    end
{% endmacro %}
