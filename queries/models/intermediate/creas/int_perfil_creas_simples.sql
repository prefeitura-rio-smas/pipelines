{{ config(materialized='ephemeral', enabled=target.name != 'prod', tags=['planilha_unificada']) }}
{{ pu_perfil_creas_sql(ref('creas')) }}
