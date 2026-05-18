{{ config(
    materialized='view'
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
),

layout_version AS (
    SELECT
        *,
        ARRAY_LENGTH(colunas) AS qtd_cols
    FROM c
),

parsed AS (
    SELECT
        -- Metadados da ingestão
        data_particao,
        qtd_cols AS qtd_colunas_brutas,

        -- Lógica Dinâmica de Mapeamento:
        -- Se o layout for de 35 colunas (2026+), o Governo inseriu uma coluna extra no início (offset 0).
        -- Portanto, deslocamos todos os offsets em +1 para layouts de 35 colunas.
        CASE 
            WHEN qtd_cols = 35 THEN TRIM(colunas[SAFE_OFFSET(1)])
            ELSE TRIM(colunas[SAFE_OFFSET(0)]) 
        END AS PROG,
        
        SAFE.PARSE_DATE('%Y%m%d', CONCAT(TRIM(colunas[SAFE_OFFSET(CASE WHEN qtd_cols = 35 THEN 2 ELSE 1 END)]), '01')) AS REF_FOLHA,
        TRIM(colunas[SAFE_OFFSET(CASE WHEN qtd_cols = 35 THEN 3 ELSE 2 END)]) AS UF,
        TRIM(colunas[SAFE_OFFSET(CASE WHEN qtd_cols = 35 THEN 4 ELSE 3 END)]) AS IBGE,
        TRIM(colunas[SAFE_OFFSET(CASE WHEN qtd_cols = 35 THEN 5 ELSE 4 END)]) AS COD_FAMILIAR,
        
        -- Colunas de identificação do beneficiário
        TRIM(colunas[SAFE_OFFSET(CASE WHEN qtd_cols = 35 THEN 6 ELSE 5 END)]) AS CPF,
        TRIM(colunas[SAFE_OFFSET(CASE WHEN qtd_cols = 35 THEN 7 ELSE 6 END)]) AS NIS,
        TRIM(colunas[SAFE_OFFSET(CASE WHEN qtd_cols = 35 THEN 8 ELSE 7 END)]) AS NOME,

        -- Colunas sobre o pagamento e benefício
        TRIM(colunas[SAFE_OFFSET(CASE WHEN qtd_cols = 35 THEN 9 ELSE 8 END)]) AS TIPO_PGTO_PREVISTO,
        TRIM(colunas[SAFE_OFFSET(CASE WHEN qtd_cols = 35 THEN 10 ELSE 9 END)]) AS PACTO,
        SAFE.PARSE_DATE('%Y%m%d', CONCAT(TRIM(colunas[SAFE_OFFSET(CASE WHEN qtd_cols = 35 THEN 11 ELSE 10 END)]), '01')) AS COMPET_PARCELA,
        TRIM(colunas[SAFE_OFFSET(CASE WHEN qtd_cols = 35 THEN 12 ELSE 11 END)]) AS TP_BENEF,
        SAFE_CAST(REPLACE(TRIM(colunas[SAFE_OFFSET(CASE WHEN qtd_cols = 35 THEN 13 ELSE 12 END)]), ',', '.') AS NUMERIC) AS VLRBENEF,
        SAFE_CAST(REPLACE(TRIM(colunas[SAFE_OFFSET(CASE WHEN qtd_cols = 35 THEN 14 ELSE 13 END)]), ',', '.') AS NUMERIC) AS VLRTOTAL,
        
        -- Colunas de situação e vigência
        TRIM(colunas[SAFE_OFFSET(CASE WHEN qtd_cols = 35 THEN 15 ELSE 14 END)]) AS SITBENEFICIO,
        TRIM(colunas[SAFE_OFFSET(CASE WHEN qtd_cols = 35 THEN 16 ELSE 15 END)]) AS SITBENEFICIARIO,
        TRIM(colunas[SAFE_OFFSET(CASE WHEN qtd_cols = 35 THEN 17 ELSE 16 END)]) AS SITFAM,
        SAFE.PARSE_DATE('%Y%m%d', TRIM(colunas[SAFE_OFFSET(CASE WHEN qtd_cols = 35 THEN 18 ELSE 17 END)])) AS INICIO_VIG_BENEF,
        SAFE.PARSE_DATE('%Y%m%d', TRIM(colunas[SAFE_OFFSET(CASE WHEN qtd_cols = 35 THEN 19 ELSE 18 END)])) AS FIM_VIG_BENEF,
        
        -- Marcadores sociais e de renda
        TRIM(colunas[SAFE_OFFSET(CASE WHEN qtd_cols = 35 THEN 20 ELSE 19 END)]) AS MARCA_RF,
        TRIM(colunas[SAFE_OFFSET(CASE WHEN qtd_cols = 35 THEN 21 ELSE 20 END)]) AS QUILOMBOLA,
        TRIM(colunas[SAFE_OFFSET(CASE WHEN qtd_cols = 35 THEN 22 ELSE 21 END)]) AS TRAB_ESCRV,
        TRIM(colunas[SAFE_OFFSET(CASE WHEN qtd_cols = 35 THEN 23 ELSE 22 END)]) AS INDIGENA,
        TRIM(colunas[SAFE_OFFSET(CASE WHEN qtd_cols = 35 THEN 24 ELSE 23 END)]) AS CATADOR_RECIC,
        TRIM(colunas[SAFE_OFFSET(CASE WHEN qtd_cols = 35 THEN 25 ELSE 24 END)]) AS TRABALHO_INF,
        SAFE_CAST(REPLACE(TRIM(colunas[SAFE_OFFSET(CASE WHEN qtd_cols = 35 THEN 26 ELSE 25 END)]), ',', '.') AS NUMERIC) AS RENDA_PER_CAPITA,
        SAFE_CAST(REPLACE(TRIM(colunas[SAFE_OFFSET(CASE WHEN qtd_cols = 35 THEN 27 ELSE 26 END)]), ',', '.') AS NUMERIC) AS RENDA_COM_PBF,
        SAFE_CAST(TRIM(colunas[SAFE_OFFSET(CASE WHEN qtd_cols = 35 THEN 28 ELSE 27 END)]) AS INTEGER) AS QTD_PESSOAS,
        
        -- Colunas cadastrais e de contato
        SAFE.PARSE_DATE('%Y%m%d', TRIM(colunas[SAFE_OFFSET(CASE WHEN qtd_cols = 35 THEN 29 ELSE 28 END)])) AS DT_ATU_CADASTRAL,
        TRIM(colunas[SAFE_OFFSET(CASE WHEN qtd_cols = 35 THEN 30 ELSE 29 END)]) AS ENDERECO,
        TRIM(colunas[SAFE_OFFSET(CASE WHEN qtd_cols = 35 THEN 31 ELSE 30 END)]) AS BAIRRO,
        TRIM(colunas[SAFE_OFFSET(CASE WHEN qtd_cols = 35 THEN 32 ELSE 31 END)]) AS CEP,
        TRIM(colunas[SAFE_OFFSET(CASE WHEN qtd_cols = 35 THEN 33 ELSE 32 END)]) AS TELEFONE1,
        TRIM(colunas[SAFE_OFFSET(CASE WHEN qtd_cols = 35 THEN 34 ELSE 33 END)]) AS TELEFONE2
    FROM layout_version
)

SELECT * FROM parsed
