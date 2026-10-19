{% macro map_coluna_tipo_curatela (coluna) %}
  CASE
    WHEN {{ coluna }} = '1' THEN 'Família extensa'
    WHEN {{ coluna }} = '2' THEN 'Público'
    WHEN {{ coluna }} = '3' THEN 'Conhecido/amigo'
    WHEN {{ coluna }} = '4' THEN 'Conselho de contabilidade'
  END
{% endmacro %}
