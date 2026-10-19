{% macro map_grau_dependencia (coluna) %}
  CASE
    WHEN {{ coluna }} = '1' THEN '1-Idoso independente'
    WHEN {{ coluna }} = '2' THEN '2-Idoso com dependencia em ate 3 atividades'
    WHEN {{ coluna }} = '3' THEN '3-Idoso com dependencia em todas as atividades'
  END
{% endmacro %}
