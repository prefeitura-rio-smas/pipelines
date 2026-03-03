{{ config(materialized = 'table') }}

WITH contas_associadas AS (
    SELECT
    us.sequs,
    us.apus as cas,
    us.emailprof AS EMAIL_UNIDADE,
    {{ map_coluna_unidade_ativa('us.indinativo') }},
    us.esfera AS ESFERA,
    us.dscus AS UNIDADE,
    us_config.numleitos AS VAGAS_TOTAIS,
    us_config.numleitosbloq  AS VAGAS_BLOQUEADAS
    FROM {{ source('cras_rma_prod', 'gh_us')}} us
    LEFT JOIN {{ source('cras_rma_prod', 'gh_us_config')}} us_config ON us_config.sequs = us.sequs
),

-- CTE para filtrar o eixo da unidade. Tem registros que estão entre vírgula. Importante fazer o unnest.
filtrar_eixo as (
  select
    t.sequs,
    trim(eixo_tratado) as eixo_tratado
  from {{ source('cras_rma_prod', 'gh_us_smas')}} as t
  left join unnest(split(ifnull(t.indeixo, ''), ',')) as eixo_tratado
),

flags as (
  select
    sequs,
    {{ map_coluna_indeixo_adulto('eixo_tratado') }} as flag_eixo_adulto,
    {{ map_coluna_indeixo_familia('eixo_tratado') }} as flag_eixo_familia,
    {{ map_coluna_indeixo_idoso('eixo_tratado') }} as flag_eixo_idoso
  from filtrar_eixo
  group by sequs
),


vagas_ocupadas AS (
    SELECT 
    sequs,
    COUNT(seqpac) AS VAGAS_OCUPADAS
    FROM {{ source('cras_rma_prod', 'gh_pac_ciclos')}}
    WHERE dtsaida IS NULL
    GROUP BY 1
),

tabela_final AS (
    SELECT
    a.UNIDADE,
    a.sequs,
    a.EMAIL_UNIDADE,
    a.cas,
    c.flag_eixo_adulto,
    c.flag_eixo_familia,
    c.flag_eixo_idoso,
    a.UNIDADE_ATIVA,
    a.ESFERA,
    a.VAGAS_TOTAIS,
    a.VAGAS_BLOQUEADAS,
    b.VAGAS_OCUPADAS,
    a.VAGAS_TOTAIS - (b.VAGAS_OCUPADAS + a.VAGAS_BLOQUEADAS) AS VAGAS_LIVRES,
    ROUND((b.VAGAS_OCUPADAS + A.VAGAS_BLOQUEADAS)/ NULLIF(a.VAGAS_TOTAIS, 0) * 100, 2) AS TAXA_OCUPACIONAL
    FROM contas_associadas a
    LEFT JOIN vagas_ocupadas b ON b.sequs = a.sequs
    left join flags c on a.sequs = c.sequs

)

select * from tabela_final
where not regexp_contains(UNIDADE, '(?i)teste')