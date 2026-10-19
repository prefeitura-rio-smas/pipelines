{% macro map_coluna_decisao_apoiada (coluna) %}
  -- Indica preenchimento cadastral do campo do processo.
  CASE
    WHEN nullif(trim(cast({{ coluna }} as string)), '') IS NULL THEN 'N'
    ELSE 'S'
  END
{% endmacro %}
