{% macro map_coluna_indeixo_adulto (coluna) %}
  max(
    case
      when lower({{ coluna }}) = 'a' then 'Sim'
      else 'Não'
    end
  )
{% endmacro %}
