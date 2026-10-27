{{ config(materialized='table', schema=var('pu_candidato_schema', 'pu_candidatos'), tags=['pu_escopos'], pre_hook='{{ pu_exigir_candidato() }}') }}
-- Uma ocorrência de resposta/presença; não agrega o histórico por usuário.
{{ pu_cpop_registros_sql() }}
