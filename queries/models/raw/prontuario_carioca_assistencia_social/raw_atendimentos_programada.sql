{{ config(materialized='table', enabled=target.name != 'prod', tags=['planilha_unificada']) }}
-- Captura atômica da seleção publicada; não recalcula nem deduplica vencedores.
select * from {{ pu_programada() }}
