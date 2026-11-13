{% macro pu_flag_sim_nao(expr) -%}
case
    when lower(trim(cast({{ expr }} as string))) in ('true', 's', 'sim') then 'Sim'
    when lower(trim(cast({{ expr }} as string))) in ('false', 'n', 'não', 'nao') then 'Não'
    else cast({{ expr }} as string)
end
{%- endmacro %}

{% macro pu_contagens_diretas(registros) %}
    {% for servico in ['cras', 'creas', 'centro_pop', 'outros'] %}
        {% if not loop.first %},{% endif %}
        (select count(*) from unnest({{ registros }}) as evento where evento.tipo_servico = '{{ servico }}') as quantidade_{{ servico }}
    {% endfor %}
{% endmacro %}
