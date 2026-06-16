-- PoC: Indicador "acompanhado pelo PAIF"
-- Granularidade: 1 linha por pessoa (id_paciente)
-- Origem: evoluções PLAN_ACOMP_FAM (codabapac=7) + serviço assistencial PAIF (id=1)

with evolucoes_paif as (
    -- Filtra apenas evoluções do módulo Plano de Acompanhamento Familiar
    select
        id_paciente,
        id_familia,
        id_unidade,
        id_profissional,
        data_evolucao,
        tipo_evolucao
    from {{ ref('raw_evolucoes_familias') }}
    where modulo_prontuario = 7
),

evolucoes_agregadas as (
    select
        id_paciente,
        id_familia,
        id_unidade,
        count(*) as total_evolucoes,
        min(data_evolucao) as data_primeira_evolucao,
        max(data_evolucao) as data_ultima_evolucao,
        -- Último profissional que atendeu
        max_by(id_profissional, data_evolucao) as id_profissional,
        -- Flag de desligamento
        bool_or(tipo_evolucao = 'D') as flag_desligado,
        -- Data do desligamento (se houver)
        min(case when tipo_evolucao = 'D' then data_evolucao end) as data_desligamento
    from evolucoes_paif
    group by id_paciente, id_familia, id_unidade
),

servico_paif as (
    -- Filtra famílias com serviço assistencial PAIF ativo
    select distinct
        m.id_paciente,
        m.id_familia,
        s.id_unidade
    from {{ ref('raw_familias_servicos_assistenciais') }} s
    inner join {{ ref('raw_membros_familia') }} m on s.id_familia = m.id_familia
    where s.id_servico_assistencial = 1
),

unidades as (
    select
        id_unidade,
        nome_unidade,
        id_tipo_unidade as tipo_unidade
    from {{ ref('raw_unidades') }}
),

combinado as (
    select
        coalesce(e.id_paciente, s.id_paciente) as id_paciente,
        coalesce(e.id_familia, s.id_familia) as id_familia,
        coalesce(e.id_unidade, s.id_unidade) as id_unidade,
        -- Origens
        e.id_paciente is not null as flag_paif_evolucao,
        s.id_paciente is not null as flag_paif_servico,
        -- Métricas da evolução
        e.total_evolucoes,
        e.data_primeira_evolucao,
        e.data_ultima_evolucao,
        e.flag_desligado,
        e.data_desligamento,
        e.id_profissional
    from evolucoes_agregadas e
    full outer join servico_paif s
        on e.id_paciente = s.id_paciente
        and e.id_familia = s.id_familia
)

select
    c.*,
    -- Origem categorizada
    case
        when c.flag_paif_evolucao and c.flag_paif_servico then 'ambos'
        when c.flag_paif_evolucao then 'evolucao'
        when c.flag_paif_servico then 'servico'
    end as flag_paif_origem,
    -- Dados da unidade
    u.nome_unidade,
    u.tipo_unidade
from combinado c
left join unidades u on c.id_unidade = u.id_unidade
