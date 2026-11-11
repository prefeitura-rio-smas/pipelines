{{ config(schema='relatorio', materialized='incremental', full_refresh=false, post_hook='alter table {{ this }} set options(expiration_timestamp=null)', partition_by={'field': 'mes_referencia', 'data_type': 'date', 'granularity': 'month'}, tags=['centro_pop_mensal']) }}
-- Histórico append-only, escrito exclusivamente pela operação manual de fechamento.
select {{ pu_cpop_mensal_colunas('m') }} from {{ ref('centro_pop_mensal') }} as m
where false
