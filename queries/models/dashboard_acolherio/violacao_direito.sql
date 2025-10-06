{{ config(materialized = 'table') }}

-- Tabela para tratar os dados antes de explodir os registros
WITH tratar_violacoes_direito AS (
    SELECT 
    NOME_USUARIO,
    ID_USUARIO,
    NULLIF(TRIM(VIOLACAO_DIREITO),  '') AS VIOLACAO_DIREITO
    FROM {{ ref('relatorio_geral')}}
    WHERE VIOLACAO_DIREITO != 'N'
),

-- Tabela para 'explodir' os registros que contém mais de um tipo de deficiência
tipo_violacao_direito_unnest AS (
    SELECT
    NOME_USUARIO,
    ID_USUARIO,
    VIOLACAO_DIREITO
    FROM tratar_violacoes_direito,
    UNNEST(SPLIT(VIOLACAO_DIREITO, ',')) AS VIOLACAO_DIREITO
),

-- Tabela para fazer a equivalência do tipo de deficiência
tabela_auxiliar_violacao_direito_tratada AS (
    SELECT
    NOME_USUARIO,
    ID_USUARIO,
    {{ map_coluna_violacao_de_direito('VIOLACAO_DIREITO') }}
    FROM tipo_violacao_direito_unnest
)

SELECT * FROM tabela_auxiliar_violacao_direito_tratada
WHERE VIOLACAO_DIREITO IS NOT NULL