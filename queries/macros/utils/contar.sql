{# Contagem genérica e reutilizável.
   contar(expr)                    -> count(distinct expr)
   contar(expr, 'condicao')        -> count(distinct case when ... then expr end)
   contar(expr, 'condicao', false) -> count(case when ... then expr end)
   contar('*', none, false)        -> count(*)
   contar('*', 'condicao', false)  -> count(case when ... then 1 end) #}
{% macro contar(expr, condicao=none, distinto=true) %}
    {% set expressao_contagem = '1' if expr | trim == '*' else expr %}
    {% if expr | trim == '*' and distinto %}
        {% set mensagem_erro = "contar: '*' exige distinto=false" %}
        {{ exceptions.raise_compiler_error(mensagem_erro) }}
    {% endif %}
    count(
        {% if distinto %}distinct {% endif %}
        {% if condicao is not none %}
            case when {{ condicao }} then {{ expressao_contagem }} end
        {% else %}
            {{ expr }}
        {% endif %}
    )
{% endmacro %}
