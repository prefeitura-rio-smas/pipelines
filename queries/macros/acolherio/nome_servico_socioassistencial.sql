{% macro nome_servico_socioassistencial(coluna) %}
    case {{ coluna }}
        when 1 then 'PAIF'
        when 6 then 'PAEFI'
        else format('Serviço não mapeado (ID %d)', {{ coluna }})
    end
{% endmacro %}
