{% macro map_coluna_situacao_de_rua (coluna) %}
  CASE
      WHEN {{ coluna }} = '5' THEN 'S'
      ELSE 'N'
  END
{% endmacro %}
