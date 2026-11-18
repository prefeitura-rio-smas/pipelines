{{ config(enabled=target.name != 'prod', materialized='ephemeral', tags=['centro_pop_mensal', 'planilha_unificada']) }}
-- A reconstrução usa os dados disponíveis a cada execução.
select * from {{ ref('int_centro_pop_mensal') }}
