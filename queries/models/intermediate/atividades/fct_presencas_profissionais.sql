-- Fato: Presenças de Profissionais em Atividades de Grupo
-- Grão: 1 linha por presença de profissional em uma atividade
-- Nota: INNER JOIN com dim_profissionais filtra profissionais órfãos (sem cadastro na dimensão)

with presencas as (
    select * from {{ ref('raw_presencas_profissionais') }}
),
profissionais as (
    select id_profissional from {{ ref('dim_profissionais') }}
    where lower(nome) not like '%teste%'
)
select
    {{ dbt_utils.generate_surrogate_key(['p.id_atividade', 'p.id_profissional', 'p.data_presenca', 'p.hora_presenca']) }} as id_presenca,
    p.id_atividade,
    p.id_profissional,
    p.data_presenca,
    p.hora_presenca,
    p.codigo_cbo
from presencas p
inner join profissionais pr on p.id_profissional = pr.id_profissional
