{{ config(materialized='table', schema=var('pu_candidato_schema', 'pu_candidatos'), tags=['pu_dimensoes'], pre_hook='{{ pu_exigir_candidato() }}') }}
{{ pu_dimensao_sql('creas') }}
