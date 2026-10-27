{{ config(materialized='table', schema=var('pu_candidato_schema', 'pu_candidatos'), tags=['pu_dimensoes']) }}
-- Último perfil conhecido por usuário e assunto, considerando todas as unidades.
{{ pu_cpop_perfil_sql() }}
