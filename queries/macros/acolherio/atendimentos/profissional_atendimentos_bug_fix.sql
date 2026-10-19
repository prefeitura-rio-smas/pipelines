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
