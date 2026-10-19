{% macro map_coluna_perfil_acesso (coluna) %}
  CASE
    WHEN {{ coluna }} = 1 THEN 'Profissional de Nível Médio'
    WHEN {{ coluna }} = 2 THEN 'Profissional de Nível Superior'
    WHEN {{ coluna }} = 3 THEN 'Gestor local'
    WHEN {{ coluna }} = 4 THEN 'Relatório'
    WHEN {{ coluna }} = 5 THEN 'Gestor Local / Prof. Nível Superior'
    WHEN {{ coluna }} = 6 THEN 'Assessoria'
    WHEN {{ coluna }} = 7 THEN 'Relatorio'
    WHEN {{ coluna }} = 8 THEN 'Acesso Personalizado'
    WHEN {{ coluna }} = 98 THEN 'Gestor central'
    WHEN {{ coluna }} = 99 THEN 'MASTER'
    ELSE 'Nível ' || CAST({{ coluna }} AS STRING)
  END
{% endmacro %}
