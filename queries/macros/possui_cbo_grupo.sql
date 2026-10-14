{% macro possui_cbo_grupo(codigos_cbo, grupo) %}
    exists (
        select 1
        from unnest({{ codigos_cbo }}) as codigo_cbo
        where starts_with(trim(codigo_cbo), '{{ grupo }}')
    )
{% endmacro %}
