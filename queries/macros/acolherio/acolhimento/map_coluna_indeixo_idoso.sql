{% macro map_coluna_indeixo_idoso (coluna) %}
  max(
    case
      when lower({{ coluna }}) = 'i' then 'Sim'
      else 'Não'
    end
  )
{% endmacro %}
