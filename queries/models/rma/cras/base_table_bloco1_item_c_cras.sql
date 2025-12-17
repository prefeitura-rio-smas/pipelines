/*
Esta tabela pega todos os membros das famílias inseridas no ACOMPANHAMENTO PAIF do mês alvo.
*/

{{ config(materialized='table') }}

-- Filtrando pelo mes alvo
WITH filtro_mes AS (
SELECT 
  *
FROM {{ ref('base_table_bloco1_item_a_cras')}}
WHERE mes_cadastro = 11
),

-- Verifica todos os membros de cada família
membro_familia  AS (
SELECT 
a.seqfamil,
b.seqmembro,
b.seqpac,
b.datsaida
from filtro_mes a 
LEFT JOIN rj-smas.brutos_acolherio_staging.gh_familias_membros b ON a.seqfamil = b.seqfamil
),

-- Pega os membros da família e suas violações de direito
membros_violacao_direito AS (
SELECT 
a.seqfamil,
a.seqmembro,
a.seqpac,
b.VIOLACAO_DIREITO,
b.deficiencia,
b.situacao_de_rua
FROM membro_familia a
INNER JOIN rj-smas-dev.dashboard_acolherio.relatorio_geral b ON b.id_usuario = a.seqpac
WHERE datsaida IS NULL
),

-- Pega a unidade responsável por aquele registro.
filtro_unidade AS (
SELECT 
a.seqfamil,
a.seqmembro,
a.seqpac,
a.violacao_direito,
a.deficiencia,
a.situacao_de_rua,
c.dscus,
b.datnascim AS data_nascimento
FROM membros_violacao_direito a
INNER JOIN rj-smas.brutos_acolherio_staging.gh_cidadao_pac b ON a.seqpac = b.seqpac
LEFT JOIN rj-smas.brutos_acolherio_staging.gh_us c ON b.sequsref = c.sequs
)

--Query final com a idade de cada membro
SELECT 
*,
DATE_DIFF(CURRENT_DATE(), data_nascimento, YEAR) AS idade
FROM filtro_unidade