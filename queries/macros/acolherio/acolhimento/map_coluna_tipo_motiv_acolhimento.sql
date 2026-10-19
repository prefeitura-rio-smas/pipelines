{% macro map_coluna_tipo_motiv_acolhimento (coluna) %}
  CASE
    WHEN {{ coluna }} = '1' THEN 'Negligência'
    WHEN {{ coluna }} = '2' THEN 'Abandono'
    WHEN {{ coluna }} = '3' THEN 'Maus tratos'
    WHEN {{ coluna }} = '4' THEN 'Conflito familiar'
    WHEN {{ coluna }} = '5' THEN 'Uso abusivo de drogas'
    WHEN {{ coluna }} = '6' THEN 'Perdido da família'
    WHEN {{ coluna }} = '7' THEN 'Violência Sexual'
    WHEN {{ coluna }} = '8' THEN 'Violência Psicológica'
  END
{% endmacro %}
