{% macro map_coluna_tipo_deficiencia (coluna) %}
  CASE
    WHEN {{ coluna }} = '1' THEN 'Deficiência auditiva total'
    WHEN {{ coluna }} = '2' THEN 'Deficiência visual parcial'
    WHEN {{ coluna }} = '3' THEN 'Deficiência visual total'
    WHEN {{ coluna }} = '4' THEN 'Deficiência motora'
    WHEN {{ coluna }} = '5' THEN 'Deficiência mental ou intelectual'
    WHEN {{ coluna }} = '6' THEN 'Deficiência auditiva parcial'
  END
{% endmacro %}
