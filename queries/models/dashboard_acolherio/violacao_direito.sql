{{ config(materialized = 'table') }}

-- Tabela para tratar os dados antes de explodir os registros
WITH tratar_violacoes_direito AS (
    SELECT 
    NOME_USUARIO,
    ID_USUARIO,
    NULLIF(TRIM(VIOLACAO_DIREITO),  '') AS violacao_direito
    FROM {{ ref('relatorio_geral')}}
    WHERE VIOLACAO_DIREITO != 'N'
),

-- Tabela para 'explodir' os registros que contém mais de um tipo de deficiência
tipo_violacao_direito_unnest AS (
    SELECT
    NOME_USUARIO,
    ID_USUARIO,
    violacao_individual
    FROM tratar_violacoes_direito
    CROSS JOIN UNNEST(SPLIT(violacao_direito, ',')) AS violacao_individual
),

-- Tabela para fazer a equivalência do tipo de deficiência
tabela_auxiliar_violacao_direito_tratada AS (
    SELECT
    NOME_USUARIO as nome_usuario,
    ID_USUARIO as seqpac,
    {{ map_coluna_violacao_de_direito('violacao_individual') }} as viol_direito
    FROM tipo_violacao_direito_unnest
)

SELECT 
    seqpac,
    nome_usuario,
    viol_direito, 
 FROM tabela_auxiliar_violacao_direito_tratada
WHERE viol_direito IS NOT NULL