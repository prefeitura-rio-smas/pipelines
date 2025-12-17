{{ config(materialized='table') }}

/*
Query para o bloco I itens B2, B3, B4 e B5.
Não foi encontrada nenhuma opção que satisfaça os itens B1 e B6
A opção extrema pobreza não existe no sistema ainda (Iem C1)
*/

-- Tabela para resgatar o nome específico da violação de direito de cada membro da família.
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
    WHERE mes_cadastro = 11
),

-- Tabela com todos os beneficiários e seus respectivos tipos de benefícios
tipo_beneficios AS (
  SELECT 
    id_usuario,
    beneficio
  FROM rj-smas-dev.gerenciamento__dbt.tipo_beneficio
  WHERE id_usuario is not null
),

-- Tabela que retorna os usuários que estão em acompanhamento PAEFI e seus benefícios
usuarios_bolsa_familia AS (
    SELECT
        a.id_usuario,
        a.beneficio,
        b.seqmembro,
        b.seqfamil
    FROM tipo_beneficios a
    INNER JOIN membro_familia b ON a.id_usuario = b.seqpac
),

-- Tabela para consultar a respectiva unidade de cada membro
filtro_unidade AS (
SELECT 
a.seqfamil,
a.id_usuario,
a.beneficio,
c.dscus,
b.datnascim AS data_nascimento
FROM usuarios_bolsa_familia a
INNER JOIN rj-smas.brutos_acolherio_staging.gh_cidadao_pac b ON a.id_usuario = b.seqpac
LEFT JOIN rj-smas.brutos_acolherio_staging.gh_us c ON b.sequsref = c.sequs
),

-- Query para buscar famílias beneficiários do Bolsa Família (ITEM B2 - RMA CRAS)
b2 AS (
SELECT 
    dscus,
    COUNT(DISTINCT(seqfamil)) AS famil_bf_b2
FROM filtro_unidade
WHERE beneficio = 'Bolsa Família'
GROUP BY dscus
),

-- Query para buscar famílias beneficiários do Bolsa Família em descumprimento de condicionalidades (ITEM B3 - RMA CRAS)
filtro_b3 AS (
    SELECT 
        a.seqfamil,
        a.id_usuario,
        a.dscus,
        a.data_nascimento,
        b.seqvulnerab
    FROM filtro_unidade a
    INNER JOIN rj-smas.brutos_acolherio_staging.gh_famil_vulnerab b ON a.seqfamil = b.seqfamil
    WHERE beneficio = 'Bolsa Família'
),

b3 AS (
SELECT 
    dscus,
    COUNT(DISTINCT(seqfamil)) AS famil_bf_cond_b3
FROM filtro_b3
WHERE seqvulnerab = 1
GROUP BY dscus
),

-- Query para buscar famílias beneficiários do BPC-Benefício de Prestação Continuada (ITEM B4 - RMA CRAS)
b4 AS (
SELECT 
    dscus,
COUNT(DISTINCT(seqfamil)) AS famil_bpc_b4
FROM filtro_unidade
WHERE beneficio = 'BPC-Benefício de Prestação Continuada'
GROUP BY dscus
),


-- Query que reaproveita a tabela ephemeral criada para o bloco C
filtro_violacao_direito AS (
    SELECT
        a.seqfamil,
        a.seqmembro,
        a.seqpac,
        a.dscus,
        a.idade,
        b.violacao_direito
    FROM {{ ref('base_table_bloco1_item_c_cras')}} a
    INNER JOIN {{ ref('violacao_direito')}} b ON a.seqpac = b.id_usuario
),

-- Query para buscar famílias com crinças ou adoslecentes em trabalho infantil (ITEM B5 - RMA CRAS)
b5 AS (
    SELECT
        dscus,
        COUNT(DISTINCT(seqfamil)) AS famil_criancas_adoslecentes_trab_infantil_b5
    FROM filtro_violacao_direito
    WHERE violacao_direito = 'Trabalho Infantil'
    AND idade < 18
    GROUP BY dscus
),

dscus_all AS (
    SELECT dscus FROM b2
    UNION DISTINCT
    SELECT dscus FROM b3
    UNION DISTINCT 
    SELECT dscus FROM b4
    UNION DISTINCT 
    SELECT dscus FROM b5
)


SELECT
    gh_us.dscus,
    a.famil_bf_b2,
    b.famil_bf_cond_b3,
    c.famil_bpc_b4,
    d.famil_criancas_adoslecentes_trab_infantil_b5
FROM dscus_all gh_us
LEFT JOIN b2 a ON gh_us.dscus = a.dscus
LEFT JOIN b3 b ON gh_us.dscus = b.dscus
LEFT JOIN b4 c ON gh_us.dscus = c.dscus
LEFT JOIN b5 d ON gh_us.dscus = d.dscus