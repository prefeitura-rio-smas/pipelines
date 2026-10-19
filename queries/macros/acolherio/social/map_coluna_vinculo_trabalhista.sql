{% macro map_coluna_vinculo_trabalhista (coluna) %}
  CASE
    WHEN {{ coluna }} = '1' THEN 'Formal'
    WHEN {{ coluna }} = '2' THEN 'Informal'
    WHEN {{ coluna }} = '3' THEN 'Não sabe / Não lembra'
    WHEN {{ coluna }} = '4' THEN 'Não quis dizer'
  END
{% endmacro %}
