{{ config(materialized = 'table') }}

-- Tabela para tratar os dados antes de explodir os registros
WITH tratar_beneficio AS (
    SELECT 
    NOME_USUARIO,
    ID_USUARIO,
    NULLIF(TRIM(BENEFICIO),  '') AS BENEFICIO
    FROM {{ ref('relatorio_geral')}}
    WHERE BENEFICIO != 'N'
),

-- Tabela para 'explodir' os registros que contém mais de um tipo de deficiência
tipo_beneficio_unnest AS (
    SELECT
    NOME_USUARIO,
    ID_USUARIO,
    BENEFICIO
    FROM tratar_beneficio,
    UNNEST(SPLIT(BENEFICIO, ',')) AS BENEFICIO
),

-- Tabela para fazer a equivalência do tipo de deficiência
tabela_beneficio_tratada AS (
    SELECT
    NOME_USUARIO,
    ID_USUARIO,
    {{ map_coluna_beneficio('BENEFICIO') }} 
    FROM tipo_beneficio_unnest
)


SELECT * FROM tabela_beneficio_tratada
WHERE BENEFICIO IS NOT NULL

