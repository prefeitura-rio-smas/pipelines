{{ config(materialized='ephemeral', enabled=target.name != 'prod', tags=['planilha_unificada']) }}
{{ pu_familias_atendidas_sql(pu_programada()) }}
