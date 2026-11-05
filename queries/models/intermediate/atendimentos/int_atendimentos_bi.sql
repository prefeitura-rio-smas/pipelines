{{ config(materialized='table', enabled=target.name != 'prod', tags=['planilha_unificada']) }}
{{ pu_atendimentos_bi_sql() }}
