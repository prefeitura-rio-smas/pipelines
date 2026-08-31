{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if target.name == 'ci' -%}
        {#- P3 (PR #110): isolamento de schema por PR. Todo modelo do target ci vai
            para o dataset dedicado ci_<PR>__<SHA> (var schema_id passada nos
            workflows), ignorando custom_schema — mesmo comportamento do
            generate_schema_name.sql do rj-crm-registry (linhas 25-28).
            Default seguro 'gerenciamento__dbt': comandos locais/manuais que não
            passam a var resolvem para ci_gerenciamento__dbt (dataset próprio,
            sem colidir com os datasets compartilhados). -#}
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
