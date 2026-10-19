{% macro map_coluna_estado_civil (coluna) %}
  CASE
    WHEN {{ coluna }} = '1' THEN 'Solteiro'
    WHEN {{ coluna }} = '2' THEN 'Casado'
    WHEN {{ coluna }} = '3' THEN 'Viúvo'
    WHEN {{ coluna }} = '4' THEN 'Separado Judicialmente'
    WHEN {{ coluna }} = '5' THEN 'União consensual'
  END
{% endmacro %}
