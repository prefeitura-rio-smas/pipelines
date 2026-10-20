{% macro map_indicador_social(coluna) %}
    case upper(trim({{ coluna }}))
        when 'S' then 'Sim'
        when 'N' then 'Não'
        else 'Não Informado'
    end
{% endmacro %}
