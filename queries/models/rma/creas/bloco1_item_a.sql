{{ config(materialized='table') }}

/*
Esta tabela pega todos os membros das famílias inseridas no ACOMPANHAMENTO PAEFI do mês alvo.
A contagem de famílias ou indivíduos inseridos no ACOMPANHAMENTO PAEFI é feita por INDIVÍDUO

A contagem está sendo por família e não por indivíduo
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
        a.datcadastr,
        a.mes_cadastro,
        b.seqpac,
        b.datsaida
    from base_table a 
    LEFT JOIN rj-smas.brutos_acolherio_staging.gh_familias_membros b ON a.seqfamil = b.seqfamil
    WHERE seqmembro = 1
),

-- Filtrando membro indívidual de cada família
membro_ind AS (
SELECT 
a.seqfamil,
a.seqmembro,
a.seqpac,
a.mes_cadastro,
b.datnascim AS data_nascimento,
b.dscnomepac AS nome_usuario,
b.sequsref
FROM membro_familia a
INNER JOIN rj-smas.brutos_acolherio_staging.gh_cidadao_pac b ON a.seqpac = b.seqpac
WHERE datsaida IS NULL
),

retirando_testes AS (
    SELECT
        *
    FROM   membro_ind
    WHERE NOT REGEXP_CONTAINS(nome_usuario, r'(?i)teste')
),

filtro_unidade AS (
    SELECT
        a.seqfamil,
        a.seqmembro,
        a.seqpac,
        a.mes_cadastro,
        a.data_nascimento,
        a.nome_usuario,
        a.sequsref,
        b.dscus
    FROM retirando_testes a 
    LEFT JOIN rj-smas.brutos_acolherio_staging.gh_us b ON a.sequsref = b.sequs
),

-- Total de indivíduos em acompanhamento PAEFI no sistema (Item A1 do RMA - CREAS)
a1 AS (
    SELECT
        dscus,
        COUNT(DISTINCT(seqpac)) as total_paefi_sistema
    FROM filtro_unidade
    GROUP BY dscus
),

-- Select final contando por membro (Item A2 do RMA - CREAS)
a2 AS (
SELECT
    dscus,
    COUNT(DISTINCT(seqpac)) as total_novos_paefi_11
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

