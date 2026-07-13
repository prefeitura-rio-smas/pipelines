-- Mart: Presenças de Profissionais (visão enriquecida para BI)
-- Grão: 1 linha por presença de profissional em atividade

with presencas as (
    select * from {{ ref('fct_presencas_profissionais') }}
),
profissionais as (
    select * from {{ ref('dim_profissionais') }}
),
atividades as (
    select * from {{ ref('dim_atividades_grupo') }}
)
select
    p.id_presenca,
    p.id_profissional,
    pr.nome as nome_profissional,
    pr.codigos_cbo,
    pr.descricoes_cbo as nome_cbo,
    p.id_atividade,
    a.nome_atividade,
    a.nome_tipo_atividade,
    a.nome_unidade,
    p.data_presenca,
    p.hora_presenca
from presencas p
left join profissionais pr on p.id_profissional = pr.id_profissional
left join atividades a on p.id_atividade = a.id_atividade
where a.nome_unidade is not null
