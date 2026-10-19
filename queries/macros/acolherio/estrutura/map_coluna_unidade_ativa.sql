{% macro map_coluna_unidade_ativa (coluna) %}
  CASE
    WHEN {{ coluna }} = 'N' THEN 'S'
    WHEN {{ coluna }} = 'S' THEN 'N'
    ELSE 'S'
  END
{% endmacro %}
