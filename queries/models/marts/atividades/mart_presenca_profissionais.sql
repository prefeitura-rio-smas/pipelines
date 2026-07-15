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
),
filtro_email as (
    select * from {{ ref('raw_sheets_filtro_email_prontuario') }}
)
select
    p.id_presenca,
    p.id_profissional,
    upper(trim(pr.nome)) as nome_profissional,
    pr.codigos_cbo as codigo_cbo,
    pr.descricoes_cbo as nome_cbo,
    p.id_atividade,
    upper(a.nome_atividade) as nome_atividade,
    upper(a.nome_tipo_atividade) as nome_tipo_atividade,
    upper(a.nome_unidade) as nome_unidade,
    p.data_presenca,
    case 
      when length(p.hora_presenca) between 1 and 4
      then substr(lpad(p.hora_presenca, 4, '0'), 1, 2) || ':' || substr(lpad(p.hora_presenca, 4, '0'), 3, 2)
      else p.hora_presenca
    end as hora_presenca,
    fe.email as email_unidade
from presencas p
left join profissionais pr on p.id_profissional = pr.id_profissional
left join atividades a on p.id_atividade = a.id_atividade
left join filtro_email fe on a.nome_unidade = upper(fe.unidade_atendimento)
where a.nome_unidade is not null
