{% macro map_coluna_nacionalidade (coluna) %}
  CASE
    WHEN {{ coluna }} = '1' THEN 'Brasileira'
    WHEN {{ coluna }} = '2' THEN 'Brasileira'
    WHEN {{ coluna }} = '3' THEN 'Estrangeira'
  END
{% endmacro %}
