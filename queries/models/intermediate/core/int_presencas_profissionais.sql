-- Camada Intermediate: Presenças de Profissionais em Atividades de Grupo
-- Grão: 1 linha por presença de profissional em uma atividade.
-- Input da fct_presencas_profissionais; a surrogate key fica na mart.
-- Nota: INNER JOIN com int_profissionais filtra profissionais órfãos/TESTE.

with presencas as (
    select * from {{ ref('raw_presencas_profissionais') }}
),
profissionais as (
    select id_profissional from {{ ref('int_profissionais') }}
    where lower(nome) not like '%teste%'
)
select
    p.id_atividade,
    p.id_profissional,
    p.data_presenca,
    p.hora_presenca,
    p.codigo_cbo
from presencas p
inner join profissionais pr on p.id_profissional = pr.id_profissional
