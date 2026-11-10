{% macro data_ocorrencia_parse(expr) %}
case
    when regexp_contains(trim({{ expr }}), r'^\d{4}-\d{2}-\d{2}(?:[ T]\d{2}:\d{2}(?::\d{2}(?:\.\d+)?)?)?$') then safe_cast(substr(trim({{ expr }}), 1, 10) as date)
    when regexp_contains(trim({{ expr }}), r'^\d{2}/\d{2}/\d{4}(?:[ T]\d{2}:\d{2}(?::\d{2})?)?$') then safe.parse_date('%d/%m/%Y', substr(trim({{ expr }}), 1, 10))
end
{% endmacro %}
