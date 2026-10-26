{% macro normalizar_hora_atendimento(coluna) -%}
coalesce(format_time('%H:%M', case
    when regexp_contains(trim(cast({{ coluna }} as string)), r'^\d{4}$')
        then safe.parse_time('%H%M', trim(cast({{ coluna }} as string)))
    when regexp_contains(trim(cast({{ coluna }} as string)), r'^\d{1,2}:\d{2}$')
        then safe.parse_time('%H:%M', trim(cast({{ coluna }} as string)))
end), '')
{%- endmacro %}
