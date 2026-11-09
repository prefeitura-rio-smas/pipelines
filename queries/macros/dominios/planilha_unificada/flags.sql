{% macro pu_flag_sim_nao(expr) -%}
case
    when lower(trim(cast({{ expr }} as string))) in ('true', 's', 'sim') then 'Sim'
    when lower(trim(cast({{ expr }} as string))) in ('false', 'n', 'não', 'nao') then 'Não'
    else cast({{ expr }} as string)
end
{%- endmacro %}
