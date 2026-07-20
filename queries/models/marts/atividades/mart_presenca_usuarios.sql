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
),
membros_atuais as (
    select id_paciente, id_familia
    from {{ ref('raw_membros_familia') }}
    where data_saida is null
    qualify row_number() over (partition by id_paciente order by data_entrada desc) = 1
),
familia_responsavel as (
    select id_familia, upper(nome_responsavel) as nome_responsavel
    from {{ ref('dim_familias') }}
    where nome_responsavel is not null
),
evolucoes as (
    select e.*, u.id_usuario
    from {{ ref('fct_evolucoes') }} e
    left join usuarios u on e.id_usuario_sk = u.id_usuario_sk
    where e.origem_modulo = 'grupo'
),
to_de_boa_indicadores as (
    select
        ie.id_evolucao_sk,
        max(case when ie.titulo_formulario = 'Tô de Boa - Desligamento'
                 and ie.label = 'Motivo do desligamento' then ie.valor end) as motivo_desligamento,
        max(case when ie.titulo_formulario = 'Tô de Boa - Cancelamento das atividades'
                 and ie.label = 'Motivo do cancelamento' then ie.valor end) as motivo_cancelamento,
        max(case when ie.titulo_formulario = 'Tô de Boa - Justificativa de Falta'
                 and ie.label = 'Justificativa' then ie.valor end) as justificativa_falta
    from {{ extrair_campos_html_evolucao(
        source_relation = ref('fct_evolucoes'),
        id_cols = ['id_evolucao_sk', 'id_atividade', 'id_usuario_sk'],
        col_html = 'descricao_evolucao'
    ) }} ie
    where ie.titulo_formulario like 'Tô de Boa -%'
    group by ie.id_evolucao_sk
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
    coalesce(mr.nome_responsavel, u.filiacao_mae) as nome_responsavel,
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
    fe.email as email_unidade,
    case when tdi.motivo_desligamento is not null then 'Sim' else 'Não' end as flag_desligamento,
    tdi.motivo_desligamento,
    case when tdi.motivo_cancelamento is not null then 'Sim' else 'Não' end as flag_cancelamento_atividades,
    tdi.motivo_cancelamento,
    case when tdi.justificativa_falta is not null then 'Sim' else 'Não' end as flag_justificativa_falta,
    tdi.justificativa_falta
from presencas p
left join usuarios u on p.id_usuario = u.id_usuario
left join atividades a on p.id_atividade = a.id_atividade
left join filtro_email fe on a.nome_unidade = upper(fe.unidade_atendimento)
left join membros_atuais ma on p.id_usuario = ma.id_paciente
left join familia_responsavel mr on ma.id_familia = mr.id_familia
left join evolucoes e on p.id_usuario = e.id_usuario and p.id_atividade = e.id_atividade
left join to_de_boa_indicadores tdi on e.id_evolucao_sk = tdi.id_evolucao_sk
