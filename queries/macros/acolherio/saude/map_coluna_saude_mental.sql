{% macro map_coluna_saude_mental(coluna ) %}
  CASE
      WHEN {{ coluna }} = 'A' THEN 'Pessoa com aparente agravo de saúde mental'
      WHEN {{ coluna }} = 'D' THEN 'Pessoa com diagnóstico (laudo médico) de doença mental'
      ELSE {{ coluna }}
  END
{% endmacro %}
