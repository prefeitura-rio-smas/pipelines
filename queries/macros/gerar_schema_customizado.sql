{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set schema = custom_schema_name | trim if custom_schema_name is not none else target.schema -%}
    {%- if target.name == 'ci' and env_var('DBT_SCHEMA_PREFIX', '') -%}
        {{ env_var('DBT_SCHEMA_PREFIX') }}_{{ schema }}
    {%- else -%}
        {{ schema }}
    {%- endif -%}
{%- endmacro %}