{{ config(materialized='ephemeral', enabled=target.name != 'prod', tags=['planilha_unificada']) }}
-- Endereço das mesmas linhas selecionadas no dbt; cadastro para os demais usuários.
{{ pu_bi_enderecos_sql() }}
