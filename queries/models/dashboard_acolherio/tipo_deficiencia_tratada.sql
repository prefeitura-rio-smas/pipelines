{{ config(materialized = 'table') }}

-- Tabela para tratar os dados antes de explodir os registros
WITH tratar_deficiencias AS (
    SELECT 
    NOME_USUARIO,
    ID_USUARIO,
    NULLIF(TRIM(TIPO_DEFICIENCIA),  '') AS TIPO_DEFICIENCIA
    FROM {{ ref('relatorio_geral')}}
    WHERE TIPO_DEFICIENCIA != 'N'
),

-- Tabela para 'explodir' os registros que contém mais de um tipo de deficiência
tipo_deficiencia_unnest AS (
    SELECT
    NOME_USUARIO,
    ID_USUARIO,
    TIPO_DEFICIENCIA
    FROM tratar_deficiencias,
    UNNEST(SPLIT(TIPO_DEFICIENCIA, ',')) AS TIPO_DEFICIENCIA
),

-- Tabela para fazer a equivalência do tipo de deficiência
tabela_auxiliar_deficiencia_tratada AS (
    SELECT
    NOME_USUARIO,
    ID_USUARIO,
    {{ map_coluna_tipo_deficiencia('TIPO_DEFICIENCIA') }} 
    FROM tipo_deficiencia_unnest
)


SELECT * FROM tabela_auxiliar_deficiencia_tratada
WHERE TIPO_DEFICIENCIA IS NOT NULL

