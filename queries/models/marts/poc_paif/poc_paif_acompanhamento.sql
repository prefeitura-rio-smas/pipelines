-- PoC: Indicador "acompanhado pelo PAIF"
-- Granularidade: 1 linha por pessoa (id_paciente)
-- Origem: evoluções fct_evolucoes (modulo_prontuario=7) + dim_familias (servico PAIF id=1)
--
-- Dependências:
--   - fct_evolucoes       (intermediate/core) — expandido com id_familia e modulo_prontuario
--   - dim_familias        (intermediate/core) — com servicos aninhados
--   - raw_membros_familia (raw)               — granularidade pessoa
--   - dim_unidades        (intermediate/core) — nome e tipo da unidade

with evolucoes_paif as (
    -- Filtra evoluções do módulo Plano de Acompanhamento Familiar
    select
        id_evolucao_sk,
        id_usuario_sk,
        id_profissional_sk,
        id_unidade_sk,
        id_familia,
        data_evolucao,
        tipo_evolucao
    from {{ ref('fct_evolucoes') }}
    where modulo_prontuario = 7
),

evolucoes_agregadas as (
    select
        id_familia,
        count(*) as total_evolucoes,
        min(data_evolucao) as data_primeira_evolucao,
        max(data_evolucao) as data_ultima_evolucao,
        max_by(id_profissional_sk, data_evolucao) as id_profissional_sk,
        max_by(id_unidade_sk, data_evolucao) as id_unidade_sk,
        LOGICAL_OR(tipo_evolucao = 'D') as flag_desligado,
        min(case when tipo_evolucao = 'D' then data_evolucao end) as data_desligamento
    from evolucoes_paif
    group by id_familia
),

familias_com_evolucao as (
    -- Famílias com evolução PAIF, expandidas por membro (granularidade pessoa)
    select
        m.id_paciente,
        e.id_familia,
        e.total_evolucoes,
        e.data_primeira_evolucao,
        e.data_ultima_evolucao,
        e.flag_desligado,
        e.data_desligamento,
        e.id_profissional_sk,
        e.id_unidade_sk
    from evolucoes_agregadas e
    inner join {{ ref('raw_membros_familia') }} m on e.id_familia = m.id_familia
),

servico_paif as (
    -- Famílias com serviço assistencial PAIF, expandidas por membro
    select distinct
        m.id_paciente,
        f.id_familia
    from {{ ref('dim_familias') }} f,
    unnest(f.servicos) s
    inner join {{ ref('raw_membros_familia') }} m on f.id_familia = m.id_familia
    where s.id_servico_assistencial = 1
)

select
    -- Granularidade: pessoa
    coalesce(e.id_paciente, s.id_paciente) as id_paciente,
    coalesce(e.id_familia, s.id_familia) as id_familia,
    -- Origens
    e.id_paciente is not null as flag_paif_evolucao,
    s.id_paciente is not null as flag_paif_servico,
    -- Origem categorizada
    case
        when e.id_paciente is not null and s.id_paciente is not null then 'ambos'
        when e.id_paciente is not null then 'evolucao'
        when s.id_paciente is not null then 'servico'
    end as flag_paif_origem,
    -- Métricas da evolução
    e.total_evolucoes,
    e.data_primeira_evolucao,
    e.data_ultima_evolucao,
    e.flag_desligado,
    e.data_desligamento,
    -- Unidade (vem da última evolução ou fica nulo se só serviço)
    du.nome_unidade,
    du.nome_tipo as tipo_unidade
from familias_com_evolucao e
full outer join servico_paif s
    on e.id_paciente = s.id_paciente
left join {{ ref('dim_unidades') }} du 
    on e.id_unidade_sk = du.id_unidade_sk
