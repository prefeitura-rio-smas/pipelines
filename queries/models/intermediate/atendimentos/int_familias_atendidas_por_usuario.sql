{{ config(materialized='ephemeral', enabled=target.name != 'prod', tags=['planilha_unificada']) }}
{{ pu_familias_atendidas_sql(ref('int_atendimentos_bi')) }}
