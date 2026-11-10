{{ config(materialized='ephemeral', enabled=target.name != 'prod', tags=['planilha_unificada']) }}
-- Commas precede dynamically generated Jinja projections.
-- noqa: disable=LT04
{{ pu_perfil_centro_pop_sql(pu_ref('int_registros_centro_pop'), pu_ref('int_atendimentos_por_usuario'), pu_ref('dim_usuarios')) }}
