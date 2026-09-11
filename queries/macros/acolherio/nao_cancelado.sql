{% macro nao_cancelado(col_flag='flag_cancelado') %}
    -- Predicado canônico de registro vigente (flag null ou diferente de 'S').
    -- Ex.: where nao_cancelado() / where nao_cancelado('a.flag_cancelado')
    ({{ col_flag }} is null or {{ col_flag }} != 'S')
{% endmacro %}
