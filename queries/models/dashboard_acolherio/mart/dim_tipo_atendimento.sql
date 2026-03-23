-- Tabela dimensão do tipo de atendimento
{{ config(materialized='ephemeral') }}

select * from {{ ref('int_tipo_atendimento') }}