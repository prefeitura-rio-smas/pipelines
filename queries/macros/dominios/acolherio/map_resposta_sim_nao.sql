{% macro map_resposta_sim_nao(coluna) %}
    case
        when lower(trim({{ coluna }})) in ('s', 'sim') then 'Sim'
        when lower(trim({{ coluna }})) in ('n', 'nao', 'não') then 'Não'
        else 'Não Informado'
    end
{% endmacro %}
