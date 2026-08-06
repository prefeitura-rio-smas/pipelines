-- Mart: profissionais vinculados às unidades do sistema
-- Grão: 1 linha por (profissional × unidade).
-- Star schema fact-less: associação que referencia as dimensões
-- dim_profissionais e dim_unidades. O vínculo é feito via
-- UNNEST(ids_unidade) da dim_profissionais, eliminando a dependência
-- direta de raw_operadores_unidades.
--
-- Exclusão de unidades de teste (PRECEDENTE mart_usuarios_medidas_alternativas):
-- a dim_unidades já exclui por design unidades com nome contendo 'TESTE'
-- (WHERE nome_unidade NOT LIKE '%TESTE%'). O mart usa a dim_unidades como
-- fonte de verdade para TODAS as colunas de unidade. As colunas
-- unidades_atuacao e qtde_unidades são RECALCULADAS no próprio mart a partir
-- do array ids_unidade + dim_unidades, garantindo que nenhuma unidade de
-- teste entre na lista mesmo se a dim_profissionais for reconstruída sem o
-- filtro. Não há flags nem ifs de teste no mart.
--
-- Métricas de atendimentos/evoluções (Frente 2):
--   total_atendimentos: COUNT(DISTINCT id_atendimento_sk) da fct_atendimentos,
--     não cancelados (flag_cancelado != 'S'), agrupado por
--     (id_profissional_sk, id_unidade). A fct explodiu profissionais
--     compartilhados, então o COUNT usa a SK única (atendimento × profissional).
--   total_evolucoes: COUNT(DISTINCT id_evolucao_sk) da fct_evolucoes,
--     ativas (data_cancelamento IS NULL), agrupado por
--     (id_profissional_sk, id_unidade).
--   GRÃO DAS MÉTRICAS — POR UNIDADE DO FATO: cada atendimento/evolução é
--   contabilizado na unidade onde ocorreu (fct_*.id_unidade = unidade do
--   fato, não do vínculo do profissional). O total NÃO é propagado para
--   todas as linhas do profissional: total_atendimentos/total_evolucoes
--   representam "atendimentos/evoluções do profissional NAQUELA unidade".
--   Decisão suportada pela estrutura das fcts (id_unidade é papel único da
--   unidade do fato, documentado na fct_evolucoes e espelhado nas fcts
--   irmãs) e pelo grão da mart (profissional × unidade): propagar o total
--   por profissional para todas as linhas inflaria as métricas por unidade.
--
-- Classificação da unidade (Frente 3):
--   territorio: código da Área Programática (AP/CAS) — campo apus da gh_us,
--     "área programática ex 1.0/2.1 (sem pontuação)". Valores: '01'-'10',
--     'GE' (unidades centrais/gestão) e '' (sem AP). NÃO é a classificação CAS
--     do tipo de unidade — é o território de gestão.
--   tipo_unidade: nome_tipo da dim_unidades (ex: CRAS, CREAS, URS).
--   classe_unidade: classe E1/E2/EG/EA (Acolhimento/Média Complexidade/
--     Gestão/Administrativo).
--   esfera_unidade: rede da unidade — 'Municipal' (rede própria) ou
--     'Rede Conveniada' (campo esfera da gh_us).
--   flag_unidade_ativa: status ativo/inativo da unidade (derivada de
--     indinativo <> 'S' na raw; BOOLEAN).
--   flag_eixo_*: eixos de atendimento (Sim/Não) derivados do campo indeixo
--     da gh_us_smas (A=Adulto, F=Família, I=Idoso).

{{ config(materialized='table', tags=['daily']) }}

with profissionais as (
    select * from {{ ref('dim_profissionais') }}
),

unidades as (
    select * from {{ ref('dim_unidades') }}
),

filtro_email as (
    select * from {{ ref('raw_sheets_filtro_email_prontuario') }}
),

-- Frente 2: atendimentos não cancelados por (profissional × unidade do fato)
atendimentos_agregados as (
    select
        id_profissional_sk,
        id_unidade,
        count(distinct id_atendimento_sk) as total_atendimentos
    from {{ ref('fct_atendimentos') }}
    where coalesce(flag_cancelado, 'N') != 'S'
    group by 1, 2
),

-- Frente 2: evoluções ativas por (profissional × unidade do fato)
evolucoes_agregadas as (
    select
        id_profissional_sk,
        id_unidade,
        count(distinct id_evolucao_sk) as total_evolucoes
    from {{ ref('fct_evolucoes') }}
    where data_cancelamento is null
    group by 1, 2
),

-- Frente 1: unidades de atuação recalculadas sobre a dim_unidades
-- (exclusão de unidades de teste por design — dim_unidades filtra TESTE)
unidades_atuacao_prof as (
    select
        prof.id_profissional_sk,
        string_agg(UPPER(unid.nome_unidade), ', ' order by unid.nome_unidade) as unidades_atuacao,
        count(distinct unid.id_unidade) as qtde_unidades
    from profissionais prof
    left join unnest(prof.ids_unidade) as id_unidade
    left join unidades unid
        on unid.id_unidade = id_unidade
    group by 1
),

joined as (
    select
        {{ dbt_utils.generate_surrogate_key(['prof.id_profissional', 'unid.id_unidade']) }} as id_profissional_unidade_sk,

        -- Profissional (da dim)
        prof.id_profissional_sk,
        prof.id_profissional,
        prof.id_login,
        UPPER(prof.nome_cadastral) as nome_profissional,
        UPPER(prof.nome_operador) as nome_operador,
        prof.cpf,
        prof.email_profissional,
        prof.email_operador,
        prof.telefone,
        prof.matricula,
        prof.flag_ativo,
        prof.login_operador,
        prof.qtde_cbos,
        prof.codigos_cbo,
        prof.descricoes_cbo,
        prof.flag_multi_cbo,
        prof.data_cadastro_conta,
        prof.data_ultimo_acesso,
        prof.dias_ultimo_acesso,

        -- Perfil de acesso e status da conta
        prof.nivel_conta,
        prof.perfil_acesso,
        prof.status_conta_codigo,
        prof.status_conta,
        prof.flag_sem_conta,

        -- Unidades de atuacao: recalculadas no mart sobre a dim_unidades
        uap.unidades_atuacao,
        uap.qtde_unidades,

        -- Unidade (da dim)
        unid.id_unidade_sk,
        unid.id_unidade,
        UPPER(unid.nome_unidade) as nome_unidade,
        unid.cas as territorio,
        unid.nome_tipo as tipo_unidade,
        unid.classe as classe_unidade,

        -- Frente 3: rede, status e eixos da unidade
        unid.esfera as esfera_unidade,
        unid.flag_unidade_ativa,
        unid.flag_eixo_adulto,
        unid.flag_eixo_familia,
        unid.flag_eixo_idoso,

        -- Frente 2: métricas por (profissional × unidade do fato)
        coalesce(atd.total_atendimentos, 0) as total_atendimentos,
        coalesce(ev.total_evolucoes, 0) as total_evolucoes,

        -- Flag indicando se o profissional tem unidade valida na dim_unidades
        (unid.id_unidade IS NOT NULL) as flag_unidade_valida,

        -- Email da unidade via planilha de filtro
        fe.email as email_unidade

    from profissionais prof
    left join unnest(prof.ids_unidade) as id_unidade
    left join unidades unid
        on unid.id_unidade = id_unidade
    left join unidades_atuacao_prof uap
        on prof.id_profissional_sk = uap.id_profissional_sk
    left join filtro_email fe
        on unid.nome_unidade = upper(fe.unidade_atendimento)
    left join atendimentos_agregados atd
        on prof.id_profissional_sk = atd.id_profissional_sk
        and unid.id_unidade = atd.id_unidade
    left join evolucoes_agregadas ev
        on prof.id_profissional_sk = ev.id_profissional_sk
        and unid.id_unidade = ev.id_unidade
)

select * from joined
where not flag_sem_conta
