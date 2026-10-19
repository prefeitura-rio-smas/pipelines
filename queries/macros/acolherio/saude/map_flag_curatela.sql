{% macro map_flag_curatela (coluna) %}
  case
    when {{ coluna }} = 'S' then 'Sim'
    else 'Não'
  end
{% endmacro %}
