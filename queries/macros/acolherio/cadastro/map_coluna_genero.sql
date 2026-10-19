{% macro map_coluna_genero (coluna) %}
  CASE
    WHEN {{ coluna }} = '02' THEN 'Travesti'
    WHEN {{ coluna }} = '03' THEN 'Homem transgênero'
    WHEN {{ coluna }} = '04' THEN 'Intersexo'
    WHEN {{ coluna }} = '05' THEN 'Mulher cisgênero'
    WHEN {{ coluna }} = '06' THEN 'Homem cisgênero'
    WHEN {{ coluna }} = '07' THEN 'Não binário'
    WHEN {{ coluna }} = '98' THEN 'Outro'
    WHEN {{ coluna }} = '01' THEN 'Mulher transgênero'
    WHEN {{ coluna }} = '99' THEN 'Sem Informação'
  END
{% endmacro %}
