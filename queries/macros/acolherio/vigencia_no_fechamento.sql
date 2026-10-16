{# Estoque no fechamento: intervalo [início, encerramento), como no RMA CRAS.
   NULL no início não comprova vigência; encerramento NULL mantém o intervalo aberto. #}
{% macro vigencia_no_fechamento(inicio, encerramento, competencia) %}
    coalesce(
        date({{ inicio }}) <= {{ fim_mes(competencia) }}
        and ({{ encerramento }} is null or date({{ encerramento }}) > {{ fim_mes(competencia) }}),
        false
    )
{% endmacro %}
