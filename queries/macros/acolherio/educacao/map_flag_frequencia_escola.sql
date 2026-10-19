{% macro map_flag_frequencia_escola (coluna) %}
  case
    when {{ coluna }} = 'S' then 'Sim'
    else 'Não'
  end
{% endmacro %}
