{{ config(materialized='ephemeral', enabled=target.name != 'prod' or var('prontuario_publicacao', false), tags=['planilha_unificada']) }}
{{ pu_formularios_sql(ref('int_evolucoes_por_usuario')) }}
