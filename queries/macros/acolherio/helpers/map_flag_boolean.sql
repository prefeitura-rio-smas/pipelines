{% macro map_flag_boolean (coluna) %}
  case
    when {{ coluna }} then 'Sim'
    when not {{ coluna }} then 'Não'
    else 'Não Informado'
  end
{% endmacro %}
