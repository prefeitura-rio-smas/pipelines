-- Macros de atendimentos: nomes e regras preservados.

{% macro classificar_atendimento(profissional) %}
    case
        when {{ profissional }}.nome_atendimento_original like '%Recepção%'
            or {{ profissional }}.nome = 'ATENDIMENTO RECEPÇÃO' then 'Atendimento Recepção'
        when {{ profissional }}.flag_cbo_recepcao
            and {{ profissional }}.nome_atendimento_original like '%CadÚnico%' then 'Atendimento Recepção'
        when {{ profissional }}.flag_cbo_tecnico then 'Atendimento Técnico'
        else 'Outros Atendimentos'
    end
{% endmacro %}

{% macro map_tipo_unidade (coluna) %}
  case
    when lower({{ coluna }}) like 'albergue%' then 'ALBERGUE'
    when lower({{ coluna }}) like 'craf%' then 'CENTRAL DE RECEPÇÃO'
    when lower({{ coluna }}) like 'cras%' then 'CRAS'
    when lower({{ coluna }}) like 'creas%' then 'CREAS'
    when lower({{ coluna }}) like 'cri%' then 'CENTRAL DE RECEPÇÃO'
    when lower({{ coluna }}) like 'república%' then 'REPÚBLICA'
    when lower({{ coluna }}) like 'urs%' then 'URS'
    else 'UNIDADE CONVENIADA'
  end
{% endmacro %}

{% macro email_cas (coluna) %}
  case
    when lower({{ coluna }}) = '10' then 'cas10@prefeitura.rio'
    when lower({{ coluna }}) = '09' then 'cas9@prefeitura.rio'
    when lower({{ coluna }}) = '08' then 'cas8@prefeitura.rio'
    when lower({{ coluna }}) = '07' then 'cas7@prefeitura.rio'
    when lower({{ coluna }}) = '06' then 'cas6@prefeitura.rio'
    when lower({{ coluna }}) = '05' then 'cas5@prefeitura.rio'
    when lower({{ coluna }}) = '04' then 'cas4@prefeitura.rio'
    when lower({{ coluna }}) = '03' then 'cas3@prefeitura.rio'
    when lower({{ coluna }}) = '02' then 'cas2@prefeitura.rio'
    when lower({{ coluna }}) = '01' then 'cas1@prefeitura.rio'
  end
{% endmacro %}

{% macro profissional_atendimentos_bug_fix (coluna) %}
  case
    when trim(lower({{ coluna }})) in (
      'atendimento recepção',
      'atendimento busca ativa',
      'atendimento entrevistador social'
    )
    then 'Não informado'
    else nome_profissional
  end
{% endmacro %}

{% macro profissional_cbo(dsccbo_col) %}
case
    when lower({{ dsccbo_col }}) like 'articulador comunitário%' then 'ARTICULADOR COMUNITÁRIO'
    when lower({{ dsccbo_col }}) like 'assistente administrativo%' then 'ASSISTENTE ADMINISTRATIVO'
    when lower({{ dsccbo_col }}) like 'assistente social%' then 'ASSISTENTE SOCIAL'
    when lower({{ dsccbo_col }}) like 'educador social%' then 'EDUCADOR SOCIAL'
    when lower({{ dsccbo_col }}) like 'entrevistador social%' then 'ENTREVISTADOR SOCIAL'
    when lower({{ dsccbo_col }}) like 'pedagogo%' then 'PEDAGOGO'
    when lower({{ dsccbo_col }}) like 'psicólogo%' then 'PSICÓLOGO'
    when lower({{ dsccbo_col }}) like 'recepcionista%' then 'RECEPCIONISTA'
    else upper({{ dsccbo_col }})
end
{% endmacro %}
