{% macro map_flag_cadunico (coluna) %}
  case
    when upper(trim(cast({{ coluna }} as string))) = 'S' then 'Sim'
    when upper(trim(cast({{ coluna }} as string))) = 'N' then 'Não'
    else null
  end
{% endmacro %}
