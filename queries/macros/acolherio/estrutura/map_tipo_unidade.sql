{% macro map_tipo_unidade (coluna) %}
  case
    when lower({{ coluna }}) like 'albergue%' then 'ALBERGUE'
    when lower({{ coluna }}) like 'craf%' then 'CENTRAL DE RECEPÇÃO'
    when lower({{ coluna }}) like 'cras%' then 'CRAS'
    when lower({{ coluna }}) like 'creas%' then 'CREAS'
    when lower({{ coluna }}) like 'cri%' then 'CENTRAL DE RECEPÇÃO'
    when lower({{ coluna }}) like 'república%' then 'REPÚBLICA'
    when lower({{ coluna }}) like 'urs%' then 'URS'
    else 'UNIDADE CONVENIADA'
  end
{% endmacro %}
