{{ config(materialized='ephemeral') }}

/*
    Tabela responsável por retornar as unidades responsáveis por cada acompanhamento PAIF.
    O total é contabilizado por família, ou seja, se uma família possui 4 membros será contabilizado apenas 1 acompanhamento PAIF.
*/

-- Busca por acompanhamentos PAIF ativos em que as contas que realizaram o cadastro PAIF possuam apenas uma unidade associada.
WITH filtro_paif_ativo AS (
    SELECT 
        seqfamil,
        seqlogincad,
        datcadastr,
        seqservassist
    FROM {{ source('brutos_acolherio_staging', 'gh_famil_servassist') }}
    WHERE datcancel IS NULL
    AND seqlogincad NOT IN (SELECT seqlogin FROM {{ source('brutos_acolherio_staging', 'gh_contas_us') }} WHERE datacesso IS NOT NULL) 
),

-- Busca todos os membros de cada família
membro_familia AS (
    SELECT 
        a.seqfamil,
        a.seqlogincad,
        a.datcadastr,
        b.seqmembro,
        b.seqpac,
        b.datsaida
    FROM filtro_paif_ativo a 
    LEFT JOIN {{ source('brutos_acolherio_staging', 'gh_familias_membros') }} b ON a.seqfamil = b.seqfamil
),

-- Busca informações indivíduais de cada membro da família
membro_informacao_individual AS (
SELECT 
    a.seqfamil,
    a.seqlogincad,
    a.datcadastr,
    a.seqmembro,
    a.seqpac,
    b.dscnomepac AS nome_usuario,
    b.sequsref,
    b.datnascim AS data_nascimento
FROM membro_familia a
INNER JOIN {{ source('brutos_acolherio_staging', 'gh_cidadao_pac') }} b ON a.seqpac = b.seqpac
WHERE datsaida IS NULL -- Membro presente na família. Se não for null é porque ele não pertence mais à familia.
),

-- Remove todos os usuários que possuem a palavra 'teste'
remover_usuarios_testes as (
  SELECT
      *
  FROM  membro_informacao_individual
  WHERE NOT REGEXP_CONTAINS(nome_usuario, r'(?i)teste')
),

-- Agrupa todos os membros por família, o login que fez o cadastro do PAIF dessa família e data do cadastro. Esta função verifica se há dados duplicados 
agrupar_familia_cadastro_paif AS (
SELECT 
*, 
ROW_NUMBER() OVER (
  PARTITION BY seqfamil, seqlogincad, datcadastr
  ORDER BY seqmembro ASC
) AS quantidade
FROM remover_usuarios_testes
),

apenas_rf AS (
SELECT * FROM agrupar_familia_cadastro_paif a 
INNER JOIN rj-smas-dev.dashboard_acolherio.contas_associadas b ON a.seqlogincad = b.seqlogin
WHERE a.quantidade = 1
ORDER BY seqlogin DESC
),

-- Pegar os profissionais de cada unidade
profissionais_unidade AS (
  SELECT
    unidade,
    seqlogin,
    operador,
    profissional,
    perfil_acesso
  FROM {{ source('dashboard_acolherio', 'contas_associadas') }}
  WHERE seqlogin NOT IN (SELECT seqlogin FROM {{ source('brutos_acolherio_staging', 'gh_contas_us') }}WHERE datacesso IS NOT NULL) # Busca os login que tem mais de um acesso)
),

tabela_final_profissional_cadastrante_com_unidade AS (
SELECT 
  a.seqfamil,
  a.seqlogincad,
  a.datcadastr,
  a.seqpac,
  b.unidade,
  b.profissional,
  b.operador,
  b.perfil_acesso
  FROM apenas_rf a
  INNER JOIN profissionais_unidade b ON a.seqlogincad = b.seqlogin
)

SELECT * FROM tabela_final_profissional_cadastrante_com_unidade