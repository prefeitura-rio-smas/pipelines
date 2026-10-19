{% macro map_coluna_serie_escola (coluna) %}
  CASE
    WHEN {{ coluna }} = '1' THEN 'Educação Infantil'
    WHEN {{ coluna }} = '2' THEN 'Fundamental I - 1° Ano'
    WHEN {{ coluna }} = '3' THEN 'Fundamental I - 2° Ano'
    WHEN {{ coluna }} = '5' THEN 'Fundamental I - 3° Ano'
    WHEN {{ coluna }} = '6' THEN 'Fundamental I - 4° Ano'
    WHEN {{ coluna }} = '7' THEN 'Fundamental I - 5° Ano'
    WHEN {{ coluna }} = '8' THEN 'Fundamental II - 6° Ano'
    WHEN {{ coluna }} = '9' THEN 'Fundamental II - 7° Ano'
    WHEN {{ coluna }} = '10' THEN 'Fundamental II - 8° Ano'
    WHEN {{ coluna }} = '11' THEN 'Fundamental II - 9° Ano'
    WHEN {{ coluna }} = '12' THEN 'Ensino Médio - 1° Ano'
    WHEN {{ coluna }} = '13' THEN 'Ensino Médio - 2° Ano'
    WHEN {{ coluna }} = '14' THEN 'Ensino Médio - 3° Ano'
    WHEN {{ coluna }} = '15' THEN 'Ensino Superior'
    WHEN {{ coluna }} = '16' THEN 'PEJA I e II'
    WHEN {{ coluna }} = '17' THEN 'PEJA III e IV'
    WHEN {{ coluna }} = '18' THEN 'PEJA V'
  END
{% endmacro %}
