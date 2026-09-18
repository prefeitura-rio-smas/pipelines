{#
    Predicado de registro não cancelado.
    - Flags: nao_cancelado('flag', 'S') -> nulo ou diferente de 'S'.
    - Datas: nao_cancelado('data_cancelamento', none) -> data nula.
#}
{% macro nao_cancelado(coluna='flag_cancelado', valor_cancelado='S') %}
    {% if valor_cancelado is none %}
        {{ coluna }} is null
    {% else %}
        (
            {{ coluna }} is null
            or {{ coluna }} != '{{ valor_cancelado | replace("'", "''") }}'
        )
    {% endif %}
{% endmacro %}
