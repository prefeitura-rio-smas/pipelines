{% macro pu_participacao_atendimento(evento, profissional) -%}
    select as struct {{ evento }}.* replace (
        [{{ profissional }}] as profissionais,
        {{ profissional }}.classificacao_atendimento as classificacao_atendimento
    )
{%- endmacro %}
