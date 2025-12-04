{{ config(materialized='table') }}

/*
Esta tabela pega todos os membros das famílias inseridas no ACOMPANHAMENTO PAEFI do mês alvo.
A contagem de famílias ou indivíduos inseridos no ACOMPANHAMENTO PAEFI é feita por INDIVÍDUO
*/

-- Busca famílias com ACOMPANHAMENTO PAEFI
WITH base_table AS (
    SELECT
        indativo,
        seqfamil,
        datcancel,
        datcadastr,
        seqlogincad,
        seqservassist,
        seqlogincancel,
        seqfamilservassist,
        EXTRACT(MONTH FROM DATETIME(datcadastr)) AS mes_cadastro
    FROM rj-smas.brutos_acolherio_staging.gh_famil_servassist
    WHERE seqservassist = 6 -- Selecionando o PAEFI
    AND datcancel IS NULL -- Cancelamento do acompanhamento PAEFI (FALSE)
),

-- Busca todos os membros de cada família que está no ACOMPANHAMENTO PAEFI
membro_familia  AS (
    SELECT 
        a.seqfamil,
        b.seqmembro,
        a.mes_cadastro,
        b.seqpac,
        b.datsaida
    from base_table a 
    LEFT JOIN rj-smas.brutos_acolherio_staging.gh_familias_membros b ON a.seqfamil = b.seqfamil
),

-- Filtra unidades para cada família
filtro_unidade AS (
SELECT 
a.seqfamil,
a.seqmembro,
a.seqpac,
a.mes_cadastro,
c.dscus,
b.datnascim AS data_nascimento
FROM membro_familia a
INNER JOIN rj-smas.brutos_acolherio_staging.gh_cidadao_pac b ON a.seqpac = b.seqpac
LEFT JOIN rj-smas.brutos_acolherio_staging.gh_us c ON b.sequsref = c.sequs
WHERE datsaida IS NULL
),

-- Total de indivíduos em acompanhamento PAEFI no sistema (Item A1 do RMA - CREAS)
a1 AS (
    SELECT
        dscus,
        COUNT(*) as total_paefi_sistema
    FROM filtro_unidade
    GROUP BY dscus
),

-- Select final contando por membro (Item A2 do RMA - CREAS)
a2 AS (
SELECT
    dscus,
    COUNT(*) as total_novos_paefi_11
 FROM filtro_unidade
 WHERE mes_cadastro = 11
 GROUP BY dscus
),

dscus_all AS (
    SELECT dscus FROM a1
    UNION DISTINCT
    SELECT dscus FROM a2
)

SELECT
    a.dscus,
    b.total_paefi_sistema,
    c.total_novos_paefi_11,
FROM dscus_all a
LEFT JOIN a1 b ON a.dscus = b.dscus
LEFT JOIN a2 c ON a.dscus = c.dscus
