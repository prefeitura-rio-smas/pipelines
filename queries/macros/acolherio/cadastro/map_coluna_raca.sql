{% macro map_coluna_raca (coluna) %}
  CASE
    WHEN {{ coluna }} = '01' THEN 'Branca'
    WHEN {{ coluna }} = '02' THEN 'Preta'
    WHEN {{ coluna }} = '03' THEN 'Parda'
    WHEN {{ coluna }} = '04' THEN 'Amarelo'
    WHEN {{ coluna }} = '05' THEN 'Indígena'
  END
{% endmacro %}
