{{ config(materialized='table') }}

/*
Esta tabela pega todos os membros das famílias inseridas no ACOMPANHAMENTO PAEFI do mês alvo.
A contagem de famílias ou indivíduos inseridos no ACOMPANHAMENTO PAIF é feita por Família.

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
    WHERE seqservassist = 1 -- Selecionando o PAIF
    AND datcancel IS NULL -- Cancelamento do acompanhamento PAIF (FALSE)
),

-- Busca todos os membros de cada família que está no ACOMPANHAMENTO PAIF
membro_familia  AS (
    SELECT 
        a.seqfamil,
        b.seqmembro,
        a.mes_cadastro,
        b.seqpac,
        b.datsaida
    FROM base_table a 
    LEFT JOIN rj-smas.brutos_acolherio_staging.gh_familias_membros b ON a.seqfamil = b.seqfamil
),

-- Filtra todos os membros de cada família.
membro_ind AS (
SELECT 
    a.seqfamil,
    a.seqmembro,
    a.seqpac,
    a.mes_cadastro,
    b.dscnomepac AS nome_usuario,
    b.sequsref,
    b.datnascim AS data_nascimento
FROM membro_familia a
INNER JOIN rj-smas.brutos_acolherio_staging.gh_cidadao_pac b ON a.seqpac = b.seqpac
WHERE datsaida IS NULL -- Membro presente na família. Se não for null é porque ele não pertence mais à familia.
),


-- Retirando os usuários testes
retirando_testes AS (
    SELECT
        *
    FROM   membro_ind
    WHERE NOT REGEXP_CONTAINS(nome_usuario, r'(?i)teste')
),

-- Filtrando a unidade de cada membro
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
)

SELECT * FROM filtro_unidade