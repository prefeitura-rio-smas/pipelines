{{ config(materialized = 'table') }}

WITH contas_associadas AS (
    SELECT
    us.sequs,
    {{ map_coluna_unidade_ativa('us.indinativo') }},
    us.esfera AS ESFERA,
    us.dscus AS UNIDADE,
    us_config.numleitos AS VAGAS_TOTAIS,
    us_config.numleitosbloq  AS VAGAS_BLOQUEADAS
    FROM {{ source('brutos_acolherio_staging', 'gh_us')}} us
    LEFT JOIN {{ source('brutos_acolherio_staging', 'gh_us_config')}} us_config ON us_config.sequs = us.sequs
),

vagas_ocupadas AS (
    SELECT 
    sequs,
    COUNT(seqpac) AS VAGAS_OCUPADAS
    FROM {{ source('brutos_acolherio_staging', 'gh_pac_ciclos')}}
    WHERE dtsaida IS NULL
    GROUP BY 1
),

tabela_final AS (
    SELECT
    a.UNIDADE,
    a.UNIDADE_ATIVA,
    a.ESFERA,
    a.VAGAS_TOTAIS,
    a.VAGAS_BLOQUEADAS,
    b.VAGAS_OCUPADAS,
    a.VAGAS_TOTAIS - (b.VAGAS_OCUPADAS + a.VAGAS_BLOQUEADAS) AS VAGAS_LIVRES,
    ROUND((b.VAGAS_OCUPADAS + A.VAGAS_BLOQUEADAS)/ NULLIF(a.VAGAS_TOTAIS, 0) * 100, 2) AS TAXA_OCUPACIONAL
    FROM contas_associadas a
    LEFT JOIN vagas_ocupadas b ON b.sequs = a.sequs

)

SELECT * FROM tabela_final