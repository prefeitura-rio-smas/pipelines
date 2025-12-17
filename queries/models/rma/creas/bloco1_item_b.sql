{{ config(materialized='table') }}

/*
Query para o bloco I itens B1, B2, B3 e B6.
Não foi encontrada nenhuma opção que satisfaça os itens B4 e B5
A opção violência intrafamiliar não existe no sistema ainda (Iem C1)
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
        b.mes_cadastro,
        a.beneficio,
        b.datsaida,
        b.seqmembro,
        b.seqfamil
    FROM tipo_beneficios a
    INNER JOIN membro_familia b ON a.id_usuario = b.seqpac
    WHERE seqmembro = 1
    
),

-- Tabela para consultar a respectiva unidade de cada membro
membro_ind AS (
SELECT 
a.seqfamil,
a.mes_cadastro,
a.datsaida,
a.id_usuario,
a.seqmembro,
a.beneficio,
b.datnascim AS data_nascimento,
b.dscnomepac AS nome_usuario,
b.sequsref
FROM usuarios_bolsa_familia a
INNER JOIN rj-smas.brutos_acolherio_staging.gh_cidadao_pac b ON a.id_usuario = b.seqpac
),

retirando_testes AS (
    SELECT
        *
    FROM   membro_ind
    WHERE NOT REGEXP_CONTAINS(nome_usuario, r'(?i)teste')
    AND datsaida IS NULL
),


filtro_unidade AS (
    SELECT
        a.seqfamil,
        a.beneficio,
        a.seqmembro,
        a.id_usuario,
        a.mes_cadastro,
        a.data_nascimento,
        a.nome_usuario,
        a.sequsref,
        b.dscus
    FROM retirando_testes a 
    LEFT JOIN rj-smas.brutos_acolherio_staging.gh_us b ON a.sequsref = b.sequs
),
-- Query para buscar famílias beneficiários do Bolsa Família (ITEM B1 - RMA CREAS)
b1 AS (
SELECT 
    dscus,
COUNT(DISTINCT(seqfamil)) AS quantidade_familia_bolsa_familia
FROM filtro_unidade
WHERE beneficio = 'Bolsa Família'
GROUP BY dscus
),

-- Query para buscar famílias beneficiários do BPC-Benefício de Prestação Continuada (ITEM B2 - RMA CREAS)
b2 AS (
SELECT 
    dscus,
COUNT(DISTINCT(seqfamil)) AS quantidade_familia_bpc
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
    FROM {{ ref('base_table_bloco1_item_c')}} a
    INNER JOIN {{ ref('violacao_direito')}} b ON a.seqpac = b.id_usuario
),


b3 AS (
    SELECT
        dscus,
        COUNT(DISTINCT(seqfamil)) AS familia_criancas_adoslecentes_trab_infantil
    FROM filtro_violacao_direito
    WHERE violacao_direito = 'Trabalho Infantil'
    AND idade < 18
    GROUP BY dscus
),

b6 AS (
    SELECT
        dscus,
        COUNT(DISTINCT(seqpac)) AS pessoas_vitimas_violacao
    FROM filtro_violacao_direito
    GROUP BY dscus
),


dscus_all AS (
    SELECT dscus FROM b1
    UNION DISTINCT
    SELECT dscus FROM b2
    UNION DISTINCT 
    SELECT dscus FROM b3
    UNION DISTINCT 
    SELECT dscus FROM b6
)


SELECT
    gh_us.dscus,
    a.quantidade_familia_bolsa_familia,
    b.quantidade_familia_bpc,
    c.familia_criancas_adoslecentes_trab_infantil,
    d.pessoas_vitimas_violacao
FROM dscus_all gh_us
LEFT JOIN b1 a ON gh_us.dscus = a.dscus
LEFT JOIN b2 b ON gh_us.dscus = b.dscus
LEFT JOIN b3 c ON gh_us.dscus = c.dscus
LEFT JOIN b6 d ON gh_us.dscus = d.dscus

