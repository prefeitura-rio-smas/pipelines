{% macro classificar_atendimento(nome, profissional, cbo) %}
case
    when {{ nome }} like '%Recepção%' then 'Atendimento Recepção'
    when {{ profissional }} = 'ATENDIMENTO RECEPÇÃO' then 'Atendimento Recepção'
    when {{ cbo }} in ('Administrador', 'Articulador Comunitário', 'Assistente administrativo', 'Educador social', 'Orientador social', 'Recepcionista')
        and {{ nome }} like '%CadÚnico%' then 'Atendimento Recepção'
    when {{ cbo }} in ('Advogado', 'Assistente social', 'Pedagogo', 'Psicólogo') then 'Atendimento Técnico'
    else 'Outros Atendimentos'
end
{% endmacro %}
