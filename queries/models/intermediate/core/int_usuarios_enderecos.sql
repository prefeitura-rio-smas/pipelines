{{ config(materialized='ephemeral', enabled=target.name != 'prod', tags=['planilha_unificada']) }}
-- A presença na referência escolhe o endereço inteiro, inclusive suas lacunas.
{{ pu_bi_enderecos_sql() }}
