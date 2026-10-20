{% macro classificar_atendimento(profissional) %}
    case
        when {{ profissional }}.nome_atendimento_original like '%Recepção%'
            or trim(upper({{ profissional }}.nome_cadastral)) = 'ATENDIMENTO RECEPÇÃO' then 'Atendimento Recepção'
        when {{ profissional }}.flag_cbo_recepcao
            and {{ profissional }}.nome_atendimento_original like '%CadÚnico%' then 'Atendimento Recepção'
        when {{ profissional }}.flag_cbo_tecnico then 'Atendimento Técnico'
        else 'Outros Atendimentos'
    end
{% endmacro %}
