{% macro map_coluna_cond_estrangeiro (coluna) %}
  CASE
    WHEN {{ coluna }} = '1' THEN 'Apatrida'
    WHEN {{ coluna }} = '2' THEN 'Asilado'
    WHEN {{ coluna }} = '3' THEN 'Imigrante com autorização de residência'
    WHEN {{ coluna }} = '4' THEN 'Imigrante com visto temporário'
    WHEN {{ coluna }} = '5' THEN 'Refugiado (Não legalizado)'
    WHEN {{ coluna }} = '6' THEN 'Refugiado (Em solicitação)'
    WHEN {{ coluna }} = '7' THEN 'Refugiado Legalizado'
    WHEN {{ coluna }} = '8' THEN 'Turista'
  END
{% endmacro %}
