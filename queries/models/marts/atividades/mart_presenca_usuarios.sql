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
filtro_email as (
    select * from {{ ref('raw_sheets_filtro_email_prontuario') }}
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
    coalesce(nullif(upper(trim(u.bairro)), ''), 'NÃO INFORMADO') as bairro,
    u.cpf as cpf,
    case when u.cpf is not null and u.cpf != '' then 'Sim' else 'Não' end as tem_cpf,
    case 
      when exists (
        select 1 from unnest(u.projetos_sociais) as ps
        where ps.id_projeto_social = 32 and ps.indicador_ativo = 'S'
      ) then 'Sim' else 'Não' 
    end as flag_to_de_boa,
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
left join usuarios u on p.id_usuario = u.id_usuario
left join atividades a on p.id_atividade = a.id_atividade
left join filtro_email fe on a.nome_unidade = upper(fe.unidade_atendimento)
