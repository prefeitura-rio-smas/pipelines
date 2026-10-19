{% macro map_coluna_motivo_desligamento (coluna) %}
  CASE
    WHEN {{ coluna }} = '2' THEN 'Deslig. por decisão da direção e/ou equipe téc. por conflito com prof. da unid. de acolhim.'
    WHEN {{ coluna }} = '3' THEN 'Deslig. por decisão da direção e/ou equipe téc. por não aceitação das regras da instituição'
    WHEN {{ coluna }} = '4' THEN 'Deslig. volunt. por conflito com outro acolhido'
    WHEN {{ coluna }} = '5' THEN 'Deslig. volunt. por conflito com prof. da unid. de acolhim.'
    WHEN {{ coluna }} = '7' THEN 'Deslig. volunt. por não aceitação das regras da instituição'
    WHEN {{ coluna }} = '8' THEN 'Deslig. volunt. por abstinência de substâncias psicoativas'
    WHEN {{ coluna }} = 'C' THEN 'Deslig. por decisão da direção e/ou equipe téc. por conflito com outro acolhido'
    WHEN {{ coluna }} = 'D' THEN 'Deslig. volunt. sem motivo identificado'
    WHEN {{ coluna }} = 'E' THEN 'Acolhido'
    WHEN {{ coluna }} = 'F' THEN 'Deslig. por reinserção comunitária'
    WHEN {{ coluna }} = 'G' THEN 'Deslig. por reinserção em família de origem ou família extensa'
    WHEN {{ coluna }} = 'H' THEN 'Deslig. por afastamento de cri/adol por medida protetiva de acolhimento'
    WHEN {{ coluna }} = 'I' THEN 'Solicitação de vaga'
    WHEN {{ coluna }} = 'J' THEN 'Reinserção através do projeto de volta a terra natal'
    WHEN {{ coluna }} = 'K' THEN 'Mudança para outro município'
    WHEN {{ coluna }} = 'L' THEN 'Transferência para clínica de apoio a saúde'
    WHEN {{ coluna }} = 'N' THEN 'Transferência para delegacia policial'
    WHEN {{ coluna }} = 'O' THEN 'Óbito'
    WHEN {{ coluna }} = 'T' THEN 'Deslig. por transferência para outra unid. de acolhim.'
    WHEN {{ coluna }} = 'X' THEN 'Fechado pela Unificação'
    WHEN {{ coluna }} = 'Y' THEN 'Demanda por serviço diurno'
    WHEN {{ coluna }} = 'Z' THEN 'Demanda por serviço noturno'

  END
{% endmacro %}
