{% macro map_flag_trabalho (coluna) %}
  case
    when {{ coluna }} = 'S' then 'Sim'
    else 'Não'
  end
{% endmacro %}
