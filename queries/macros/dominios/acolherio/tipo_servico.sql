{% macro pu_tipo_servico(expressao) %}
    case {{ expressao }} when 'CRAS' then 'cras' when 'CREAS' then 'creas'
        when 'Centro POP' then 'centro_pop' else 'outros' end
{% endmacro %}
