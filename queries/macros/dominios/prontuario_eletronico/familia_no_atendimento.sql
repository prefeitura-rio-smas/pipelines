{% macro pu_ids_familias(coluna) -%}
    coalesce(string_agg(distinct cast({{ coluna }} as string), ', ' order by cast({{ coluna }} as string)), '')
{%- endmacro %}
