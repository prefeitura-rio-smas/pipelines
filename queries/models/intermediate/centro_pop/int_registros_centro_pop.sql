{{ config(materialized='table', tags=['planilha_unificada', 'pu_escopos']) }}
-- Uma ocorrência de resposta/presença; não agrega o histórico por usuário.
{{ registros_centro_pop() }}
