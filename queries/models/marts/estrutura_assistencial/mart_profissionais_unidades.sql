-- Mart: profissionais vinculados às unidades do sistema.
-- Grão: 1 linha por (profissional × unidade). Fact-less star schema que
-- referencia dim_profissionais e dim_unidades (vínculo via UNNEST).
-- Unidades de teste excluídas por design nas dims; métricas contadas na
-- unidade do fato (não propagadas por profissional). Email = filtro único
-- de BI consolidado na dim_unidades (email_filtro). Detalhes no YML.

{{ config(materialized='table', tags=['daily']) }}

with profissionais as (
    select * from {{ ref('dim_profissionais') }}
),

unidades as (
    select * from {{ ref('dim_unidades') }}
),

-- Métricas por (profissional × unidade do fato)
atendimentos_agregados as (
    select
        id_profissional_sk,
        id_unidade,
        count(distinct id_atendimento_sk) as total_atendimentos
    from {{ ref('fct_atendimentos') }}
    where coalesce(flag_cancelado, 'N') != 'S'
    group by 1, 2
),

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
        prof.nivel_conta,
        prof.perfil_acesso,
        prof.status_conta_codigo,
        prof.status_conta,
        prof.flag_sem_conta,

        -- Unidades de atuacao (da dim_profissionais)
        UPPER(prof.unidades_atuacao) as unidades_atuacao,
        prof.qtde_unidades,

        unid.id_unidade_sk,
        unid.id_unidade,
        UPPER(unid.nome_unidade) as nome_unidade,
        unid.cas as territorio,
        unid.tipo_unidade as tipo_unidade,
        unid.classe as classe_unidade,
        unid.esfera as esfera_unidade,
        unid.flag_unidade_ativa,
        unid.flag_eixo_adulto,
        unid.flag_eixo_familia,
        unid.flag_eixo_idoso,

        coalesce(atd.total_atendimentos, 0) as total_atendimentos,
        coalesce(ev.total_evolucoes, 0) as total_evolucoes,

        (unid.id_unidade IS NOT NULL) as flag_unidade_valida,

        -- Email: filtro único de BI consolidado na dim_unidades
        unid.email_filtro as email_filtro,

        {{ extrair_ultima_atualizacao('raw_configuracoes_sistema') }} as ultima_atualizacao

    from profissionais prof
    left join unnest(prof.ids_unidade) as id_unidade
    left join unidades unid
        on unid.id_unidade = id_unidade
    left join atendimentos_agregados atd
        on prof.id_profissional_sk = atd.id_profissional_sk
        and unid.id_unidade = atd.id_unidade
    left join evolucoes_agregadas ev
        on prof.id_profissional_sk = ev.id_profissional_sk
        and unid.id_unidade = ev.id_unidade
)

select *
from joined
where not flag_sem_conta
