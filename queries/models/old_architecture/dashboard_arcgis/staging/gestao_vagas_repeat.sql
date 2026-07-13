{{ config(materialized = 'ephemeral') }}

SELECT * FROM {{ source('arcgis_raw', 'gestao_vagas_repeat_raw') }}