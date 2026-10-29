{{ config(materialized='ephemeral', enabled=target.name != 'prod', tags=['planilha_unificada']) }}
-- Contexto da foto publicada; vínculo de família preservado por profissional.
{{ pu_bi_complementos_sql() }}
