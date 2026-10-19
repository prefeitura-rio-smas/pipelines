{% macro map_coluna_orientacao_sexual (coluna) %}
  CASE
    WHEN {{ coluna }} = '1' THEN 'Lésbica'
    WHEN {{ coluna }} = '2' THEN 'Gay'
    WHEN {{ coluna }} = '3' THEN 'Bissexual'
    WHEN {{ coluna }} = '4' THEN 'Heterossexual'
    WHEN {{ coluna }} = '5' THEN 'Pansexual'
    WHEN {{ coluna }} = '6' THEN 'Não informado'
  END
{% endmacro %}
