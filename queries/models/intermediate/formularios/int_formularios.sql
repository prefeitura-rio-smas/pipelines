{{ config(materialized='ephemeral', tags=['planilha_unificada']) }}
{% set evolucoes %}({{ preparar_evolucoes() }}){% endset %}
{{ pu_formularios_sql(evolucoes) }}
