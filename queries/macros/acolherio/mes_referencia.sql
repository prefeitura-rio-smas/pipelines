{% macro mes_referencia() %}
-- Primeiro dia do mês de referência dos RMAs.
-- Var competencia ('AAAA-MM'); vazio = mês corrente.
{% set competencia = var('competencia', '') %}
{% if competencia == '' %}
date_trunc(current_date(), month)
{% else %}
date('{{ competencia }}-01')
{% endif %}
{% endmacro %}
