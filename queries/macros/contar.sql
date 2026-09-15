{# Contagem genérica e reutilizável (evita repetir count(distinct if(...))).
   contar(expr)                    -> count(distinct expr)
   contar(expr, 'condicao')        -> count(distinct if(condicao, expr, null))
   contar(expr, 'condicao', false) -> count(if(condicao, expr, null))
   contar('*', none, false)        -> count(*) #}
{% macro contar(expr, condicao=none, distinto=true) %}
    count(
        {%- if distinto %} distinct{% endif %}
        {%- if condicao is not none %} if({{ condicao }}, {{ expr }}, null)
        {%- else %} {{ expr }}
        {%- endif %}
    )
{% endmacro %}
