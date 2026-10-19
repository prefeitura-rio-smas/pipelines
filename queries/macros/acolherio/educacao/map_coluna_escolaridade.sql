{% macro map_coluna_escolaridade (coluna) %}
  CASE
    WHEN {{ coluna }} = '01' THEN 'Não Alfabetizado'
    WHEN {{ coluna }} = '02' THEN 'Alfabetizado'
    WHEN {{ coluna }} = '03' THEN 'Nível fundamental incompleto'
    WHEN {{ coluna }} = '04' THEN 'Nível fundamental completo'
    WHEN {{ coluna }} = '05' THEN 'Nível médio incompleto'
    WHEN {{ coluna }} = '06' THEN 'Nível médio completo'
    WHEN {{ coluna }} = '07' THEN 'Superior incompleto'
    WHEN {{ coluna }} = '08' THEN 'Superior completo'
    -- Catálogo indescolari: escolaridade.ini e escolaridade_bc26.ini.
    WHEN {{ coluna }} = '09' THEN 'Especialização'
    WHEN {{ coluna }} = '10' THEN 'Mestrado'
    WHEN {{ coluna }} = '11' THEN 'Doutorado'
    WHEN {{ coluna }} = '12' THEN 'Nunca estudou'
    WHEN {{ coluna }} = '13' THEN 'Nível fundamental incompleto (cursando)'
    WHEN {{ coluna }} = '14' THEN 'Nível médio incompleto (cursando)'
    WHEN {{ coluna }} = '15' THEN 'Superior incompleto (cursando)'
    WHEN {{ coluna }} = '16' THEN 'Não sabe/Não informou'
  END
{% endmacro %}
