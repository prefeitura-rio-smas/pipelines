{{ config(enabled=var('pu_dimensoes_eventos_legadas', false), materialized='table', schema=var('pu_candidato_schema', 'pu_candidatos'), tags=['pu_dimensoes']) }}
{{ pu_dimensao_sql('outros') }}
