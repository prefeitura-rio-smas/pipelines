{{ config(enabled=target.name != 'prod' or var('prontuario_publicacao', false), materialized='table', schema=var('pu_candidato_schema', 'pu_candidatos'), tags=['planilha_unificada', 'pu_escopos']) }}
-- Uma ocorrência de resposta/presença; não agrega o histórico por usuário.
{{ registros_centro_pop() }}
