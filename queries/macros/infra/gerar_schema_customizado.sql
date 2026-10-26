{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if var('pu_isolar_schema', '') and target.name in ['dev', 'staging'] -%}
        {{ var('pu_isolar_schema') }}
    {%- elif target.name == 'ci' -%}
        {#- Target ci: dataset dedicado ci_<PR>__<SHA> (var schema_id; default gerenciamento__dbt p/ comandos manuais). -#}
        ci_{{ var('schema_id', 'gerenciamento__dbt') }}
    {%- else -%}
        {%- set default_schema = target.schema -%}
        {%- if custom_schema_name is none -%}
            {{ default_schema }}
        {%- else -%}
            {{ custom_schema_name | trim }}
        {%- endif -%}
    {%- endif -%}
{%- endmacro %}
