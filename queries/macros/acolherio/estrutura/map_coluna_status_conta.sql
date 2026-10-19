{% macro map_coluna_status_conta (coluna) %}
  CASE
    WHEN {{ coluna }} = '0' THEN 'Inativo'
    WHEN {{ coluna }} = '1' THEN 'Ativo'
    WHEN {{ coluna }} = '2' THEN 'Trocar senha'
    WHEN {{ coluna }} = '3' THEN 'Bloqueado'
    WHEN {{ coluna }} = '4' THEN 'Atualizar cadastro'
    WHEN {{ coluna }} = '5' THEN 'Aceitar Termo de Uso'
  END AS status_conta
{% endmacro %}
