-- Mart: Presenças de Usuários (visão enriquecida para BI)
-- Grão: 1 linha por presença de usuário em atividade
-- Todas as colunas de texto em UPPER conforme padrão do BI

with presencas as (
    select * from {{ ref('fct_presencas_usuarios') }}
),
usuarios as (
    select * from {{ ref('dim_usuarios') }}
),
atividades as (
    select * from {{ ref('dim_atividades_grupo') }}
),
-- Vínculo familiar atual do usuário para obter nome do responsável
membros_atuais as (
    select
        id_paciente,
        id_familia,
        row_number() over (
            partition by id_paciente
            order by data_entrada desc
        ) as rn
    from {{ ref('raw_membros_familia') }}
    where data_saida is null
),
familias as (
    select id_familia, nome_responsavel from {{ ref('dim_familias') }}
)
select
    p.id_presenca,
    p.id_usuario,
    upper(u.nome) as nome_usuario,
    u.data_nascimento,
    date_diff(current_date(), u.data_nascimento, year) as idade,
    u.sexo,
    u.raca_cor,
    upper(u.filiacao_mae) as filiacao_mae,
    upper(f.nome_responsavel) as nome_responsavel,
    upper(u.bairro) as bairro,
    p.id_atividade,
    upper(a.nome_atividade) as nome_atividade,
    upper(a.nome_tipo_atividade) as nome_tipo_atividade,
    upper(a.nome_unidade) as nome_unidade,
    p.data_presenca,
    p.hora_presenca
from presencas p
left join usuarios u on p.id_usuario = u.id_usuario
left join membros_atuais m on p.id_usuario = m.id_paciente and m.rn = 1
left join familias f on m.id_familia = f.id_familia
left join atividades a on p.id_atividade = a.id_atividade
