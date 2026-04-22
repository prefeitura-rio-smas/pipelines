{{ config(
    materialized='table',
    partition_by={
      "field": "data_particao",
      "data_type": "date",
      "granularity": "day"
    }
) }}

WITH source AS (
    SELECT 
        linha_bruta,
        data_particao
    FROM {{ source('bolsa_familia_staging', 'folha') }}
),

c AS (
    SELECT
        SPLIT(REPLACE(linha_bruta, '"', ''), ';') AS colunas,
        data_particao
    FROM source
    WHERE linha_bruta IS NOT NULL 
      AND TRIM(linha_bruta) != ''
      AND NOT STARTS_WITH(linha_bruta, 'PROG;')
),

parsed AS (
    SELECT
        -- Metadados da ingestão
        data_particao,

        -- Colunas de identificação e referência
        TRIM(colunas[SAFE_OFFSET(0)]) AS PROG,
        SAFE.PARSE_DATE('%Y%m%d', CONCAT(TRIM(colunas[SAFE_OFFSET(1)]), '01')) AS REF_FOLHA,
        TRIM(colunas[SAFE_OFFSET(2)]) AS UF,
        TRIM(colunas[SAFE_OFFSET(3)]) AS IBGE,
        TRIM(colunas[SAFE_OFFSET(4)]) AS COD_FAMILIAR,
        
        -- Colunas de identificação do beneficiário
        TRIM(colunas[SAFE_OFFSET(5)]) AS CPF,
        TRIM(colunas[SAFE_OFFSET(6)]) AS NIS,
        TRIM(colunas[SAFE_OFFSET(7)]) AS NOME,

        -- Colunas sobre o pagamento e benefício
        TRIM(colunas[SAFE_OFFSET(8)]) AS TIPO_PGTO_PREVISTO,
        TRIM(colunas[SAFE_OFFSET(9)]) AS PACTO,
        SAFE.PARSE_DATE('%Y%m%d', CONCAT(TRIM(colunas[SAFE_OFFSET(10)]), '01')) AS COMPET_PARCELA,
        TRIM(colunas[SAFE_OFFSET(11)]) AS TP_BENEF,
        SAFE_CAST(REPLACE(TRIM(colunas[SAFE_OFFSET(12)]), ',', '.') AS NUMERIC) AS VLRBENEF,
        SAFE_CAST(REPLACE(TRIM(colunas[SAFE_OFFSET(13)]), ',', '.') AS NUMERIC) AS VLRTOTAL,
        
        -- Colunas de situação e vigência
        TRIM(colunas[SAFE_OFFSET(14)]) AS SITBENEFICIO,
        TRIM(colunas[SAFE_OFFSET(15)]) AS SITBENEFICIARIO,
        TRIM(colunas[SAFE_OFFSET(16)]) AS SITFAM,
        SAFE.PARSE_DATE('%Y%m%d', TRIM(colunas[SAFE_OFFSET(17)])) AS INICIO_VIG_BENEF,
        SAFE.PARSE_DATE('%Y%m%d', TRIM(colunas[SAFE_OFFSET(18)])) AS FIM_VIG_BENEF,
        
        -- Marcadores sociais e de renda
        TRIM(colunas[SAFE_OFFSET(19)]) AS MARCA_RF,
        TRIM(colunas[SAFE_OFFSET(20)]) AS QUILOMBOLA,
        TRIM(colunas[SAFE_OFFSET(21)]) AS TRAB_ESCRV,
        TRIM(colunas[SAFE_OFFSET(22)]) AS INDIGENA,
        TRIM(colunas[SAFE_OFFSET(23)]) AS CATADOR_RECIC,
        TRIM(colunas[SAFE_OFFSET(24)]) AS TRABALHO_INF,
        SAFE_CAST(REPLACE(TRIM(colunas[SAFE_OFFSET(25)]), ',', '.') AS NUMERIC) AS RENDA_PER_CAPITA,
        SAFE_CAST(REPLACE(TRIM(colunas[SAFE_OFFSET(26)]), ',', '.') AS NUMERIC) AS RENDA_COM_PBF,
        SAFE_CAST(TRIM(colunas[SAFE_OFFSET(27)]) AS INTEGER) AS QTD_PESSOAS,
        
        -- Colunas cadastrais e de contato
        SAFE.PARSE_DATE('%Y%m%d', TRIM(colunas[SAFE_OFFSET(28)])) AS DT_ATU_CADASTRAL,
        TRIM(colunas[SAFE_OFFSET(29)]) AS ENDERECO,
        TRIM(colunas[SAFE_OFFSET(30)]) AS BAIRRO,
        TRIM(colunas[SAFE_OFFSET(31)]) AS CEP,
        TRIM(colunas[SAFE_OFFSET(32)]) AS TELEFONE1,
        TRIM(colunas[SAFE_OFFSET(33)]) AS TELEFONE2
    FROM c
    WHERE ARRAY_LENGTH(colunas) = 34
),

with_family_sum AS (
    SELECT 
        *,
        SUM(VLRBENEF) OVER (PARTITION BY data_particao, COD_FAMILIAR) AS beneficio_familiar_valor
    FROM parsed
)

SELECT 
    *,
    CASE 
        WHEN beneficio_familiar_valor <= 374 THEN 'ate 374'
        WHEN beneficio_familiar_valor <= 600 THEN '375 a 600'
        WHEN beneficio_familiar_valor <= 750 THEN '601 a 750'
        WHEN beneficio_familiar_valor <= 1350 THEN '751 a 1350'
        ELSE 'acima de 1350' 
    END AS beneficio_familiar_faixa
FROM with_family_sum