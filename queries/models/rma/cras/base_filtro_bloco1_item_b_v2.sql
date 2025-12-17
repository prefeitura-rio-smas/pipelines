{{ config(materialized='table') }}

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
        EXTRACT(MONTH FROM datcadastr) AS mes_cadastro_assist,
        EXTRACT(MONTH FROM CURRENT_DATE()) AS mes_atual,
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
        a.mes_cadastro_assist,
        a.mes_atual,
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
    a.mes_cadastro_assist,
    a.mes_atual,
    a.seqmembro,
    a.seqpac,
    b.dscnomepac AS nome_usuario,
    b.datnascim AS data_nascimento,
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

tipo_beneficios AS (
  SELECT 
    id_usuario,
    beneficio
  FROM {{ ref('tipo_beneficio')}}
  WHERE id_usuario IS NOT NULL
),

usuarios_com_beneficio_ativos_paif AS (
    SELECT
        a.id_usuario,
        b.data_nascimento,
        b.mes_cadastro_assist,
        b.mes_atual,
        a.beneficio,
        b.seqmembro,
        b.seqfamil,
        b.seqlogincad
    FROM tipo_beneficios a
    INNER JOIN remover_usuarios_testes b ON a.id_usuario = b.seqpac
),

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
  a.data_nascimento,
  a.mes_cadastro_assist,
  a.mes_atual,
  a.beneficio,
  a.seqmembro,
  a.seqlogincad,
  a.id_usuario,
  b.unidade,
  b.profissional,
  b.operador,
  b.perfil_acesso
  FROM usuarios_com_beneficio_ativos_paif a
  INNER JOIN profissionais_unidade b ON a.seqlogincad = b.seqlogin
)

SELECT 
    *,
    DATE_DIFF(CURRENT_DATE(), data_nascimento, YEAR) AS idade
FROM tabela_final_profissional_cadastrante_com_unidade