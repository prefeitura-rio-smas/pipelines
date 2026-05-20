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
        -- Layout de 34 colunas (2025 e anteriores): mapeamento direto.
        -- Layout de 35 colunas (2026+): o Governo inseriu uma coluna extra
        -- ENTRE TRABALHO_INF (col 24) e RENDA_PER_CAPITA (col 25 no 34-col).
        -- Portanto, colunas 0-24 têm o mesmo offset nos dois layouts;
        -- colunas 25+ (RENDA_PER_CAPITA em diante) ganham +1 no layout 35-col.
        
        -- Colunas 0-24: mesmos offsets para 34 e 35 colunas
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
        
        -- Colunas 25+: offset +1 para layout de 35 colunas
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
