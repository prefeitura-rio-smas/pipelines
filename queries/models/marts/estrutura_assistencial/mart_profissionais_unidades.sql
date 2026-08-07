-- Mart: profissionais vinculados às unidades do sistema
-- Grão: 1 linha por (profissional × unidade).
-- Star schema fact-less: associação que referencia as dimensões
-- dim_profissionais e dim_unidades. O vínculo é feito via
-- UNNEST(ids_unidade) da dim_profissionais, eliminando a dependência
-- direta de raw_operadores_unidades.
--
-- Exclusão de unidades de teste (PRECEDENTE mart_usuarios_medidas_alternativas):
-- a dim_unidades exclui por design unidades com nome contendo 'TESTE'
-- (WHERE nome_unidade NOT LIKE '%TESTE%'), e a dim_profissionais herda essa
-- exclusão via LEFT JOIN dim_unidades + WHERE id_unidade IS NOT NULL.
-- unidades_atuacao e qtde_unidades vêm da dim_profissionais (já limpas).
-- A exclusão também é garantida no mart pelo join com dim_unidades no UNNEST.
-- Não há flags nem ifs de teste no mart.
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
--   tipo_unidade: label de BI derivado do nome_tipo da dim_unidades (fonte
--     canônica gh_us_tipo/dsctipous), NÃO do nome livre da unidade. Decisão
--     com evidência (2026-08-07): o cadastro de tipos COBRE todos os tipos
--     (Albergue, Central de Recepção/CRAF/CRI, CRAS, CREAS, República, URS,
--     Centro POP, Lares Cariocas = 'Moradia Primeiro', Escritório Social),
--     então o LIKE sobre nome_unidade é desnecessário E tem falsos positivos
--     comprovados ('%CRI%' classifica 'URS RIO ACOLHEDOR - CRISTO REDENTOR'
--     como Central de Recepção; '%CRAF%'/'%CRI%'/'%URS%' classificam as
--     'INVERNO ACOLHEDOR - ...' — que são Abrigo do Frio — como Central de
--     Recepção/URS). O else preserva o tipo cadastral original (nunca
--     'Unidade Conveniada' hardcoded: é factualmente errado para Nível
--     Central, Pólo Tô de Boa, Central de Regulação etc.). Não há macro de
--     tipo_unidade reutilizável no projeto (map_tipo_unidade existe mas não é
--     usada por nenhum modelo e tem os mesmos bugs de LIKE sobre nome livre);
--     regra mantida no modelo (uso único hoje).
--   classe_unidade: classe E1/E2/EG/EA (Acolhimento/Média Complexidade/
--     Gestão/Administrativo).
--   esfera_unidade: rede da unidade — 'Municipal' (rede própria) ou
--     'Rede Conveniada' (campo esfera da gh_us).
--   flag_unidade_ativa: status ativo/inativo da unidade (derivada de
--     indinativo <> 'S' na raw; BOOLEAN).
--   flag_eixo_*: eixos de atendimento (Sim/Não) derivados do campo indeixo
--     da gh_us_smas (A=Adulto, F=Família, I=Idoso).
--
-- Filtro de email da unidade (padrão dos marts de presença, ex:
-- mart_presenca_profissionais/usuarios): LEFT JOIN com raw_sheets_
-- filtro_email_prontuario (planilha de apoio do dashboard) enriquecendo a
-- mart com o(s) email(s) de contato da unidade. JOIN com lower(trim()) nos
-- DOIS lados (padrão mart_acolhimento_diaria) — o join anterior
-- (unid.nome_unidade = upper(fe.unidade_atendimento)) deixava de casar
-- unidades com espaços/acentos divergentes na planilha (ex: unidade
-- ' ABRIGO DO FRIO - BASE DA ABORDAGEM' com espaço inicial não casava).
-- Grão preservado: verificado sem fan-out (planilha com 1 linha por unidade;
-- id_profissional_unidade_sk permanece única).

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

        -- Unidades de atuacao: da dim_profissionais (já exclui unidades de teste)
        UPPER(prof.unidades_atuacao) as unidades_atuacao,
        prof.qtde_unidades,

        -- Unidade (da dim)
        unid.id_unidade_sk,
        unid.id_unidade,
        UPPER(unid.nome_unidade) as nome_unidade,
        unid.cas as territorio,
        -- tipo_unidade: label de BI derivado do nome_tipo cadastral (fonte
        -- canônica). Regra documentada no cabeçalho do modelo.
        case
            when unid.nome_tipo is null then 'Não Informado'
            when lower(unid.nome_tipo) like 'albergue%' then 'Albergue'
            when lower(unid.nome_tipo) like 'central de recepção%' then 'Central de Recepção'
            when lower(unid.nome_tipo) like '%cras%' then 'CRAS'
            when lower(unid.nome_tipo) like '%creas%' then 'CREAS'
            when lower(unid.nome_tipo) like 'centro de ref.espec.popula%' then 'Centro POP'
            when lower(unid.nome_tipo) like 'complexo da urs%' then 'URS'
            when lower(unid.nome_tipo) like 'urs%' then 'URS'
            when lower(unid.nome_tipo) = 'república' then 'República'
            when lower(unid.nome_tipo) = 'moradia primeiro' then 'Lares Cariocas'
            -- Unidade única com nome_tipo genérico ('Unidades e Serviços de
            -- Média Complexidade'); 1:1 comprovado nos dados (só a unidade
            -- ESCRITÓRIO SOCIAL tem 'ESCRITÓRIO' no nome).
            when upper(unid.nome_unidade) like '%ESCRITÓRIO%' then 'Escritório Social'
            else unid.nome_tipo
        end as tipo_unidade,
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

        -- Email da unidade via planilha de filtro (padrão das marts de
        -- presença). Pode conter múltiplos emails separados por vírgula.
        fe.email as email_unidade

    from profissionais prof
    left join unnest(prof.ids_unidade) as id_unidade
    left join unidades unid
        on unid.id_unidade = id_unidade
    left join filtro_email fe
        on lower(trim(fe.unidade_atendimento)) = lower(trim(unid.nome_unidade))
    left join atendimentos_agregados atd
        on prof.id_profissional_sk = atd.id_profissional_sk
        and unid.id_unidade = atd.id_unidade
    left join evolucoes_agregadas ev
        on prof.id_profissional_sk = ev.id_profissional_sk
        and unid.id_unidade = ev.id_unidade
)

select * from joined
where not flag_sem_conta
