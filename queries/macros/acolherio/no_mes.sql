{% macro no_mes(col_data) %}
-- Predicado "coluna cai no mês de referência". Ex: no_mes('data_atendimento').
date_trunc({{ col_data }}, month) = {{ mes_referencia() }}
{% endmacro %}
