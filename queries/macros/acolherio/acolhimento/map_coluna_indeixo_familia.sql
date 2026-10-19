{% macro map_coluna_indeixo_familia (coluna) %}
  max(
    case
      when lower({{ coluna }}) = 'f' then 'Sim'
      else 'Não'
    end
  )
{% endmacro %}
