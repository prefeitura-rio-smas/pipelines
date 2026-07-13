-- Mart: Rastreio de usuarios por unidade (simplificado)
-- Granularidade: 1 linha por (id_paciente, id_unidade)
--
-- Consolida metricas de interacao (atendimentos + evolucoes) em cada unidade.
-- Foco exclusivo no detalhamento por unidade especifica.
-- Removidas flags PAIF, PAEFI e vulnerabilidades conforme solicitacao do usuario.
-- Adicionados campos de enriquecimento: nome, sexo, idade, violacoes_descricao.
--
-- Evolucoes:
--   1. Diretas: via id_usuario_sk (origem usuario/administrativa)
--   2. Familiares: via id_paciente_familia (membro especifico da familia)
--   Nota: fct_evolucoes ja possui id_paciente_familia preenchido para
--   todas as evolucoes familiares, eliminando necessidade de expansao
--   via raw_membros_familia.
--   Filtro: exclui evolucoes com descricao vazia.
--
-- Atendimentos:
--   Via fct_atendimentos, filtrando registros cancelados.
--
-- Dependencias:
--   - fct_atendimentos                  (intermediate/core)
--   - fct_evolucoes                     (intermediate/core)
--   - dim_usuarios                      (intermediate/core)
--   - dim_unidades                      (intermediate/core)
--   - raw_membros_familia               (raw - apenas para id_familia atual)

{{ config(
    materialized='table',
    schema='relatorio',
    tags=['risco_social']
) }}

with
    evolucoes_por_pessoa_unidade as (
        -- Parte 1: Evolucoes diretas do usuario (origem usuario/administrativa)
        select
            du.id_usuario as id_paciente,
            dun.id_unidade,
            fct.data_evolucao as data_interacao
        from {{ ref('fct_evolucoes') }} fct
        inner join {{ ref('dim_usuarios') }} du
            on fct.id_usuario_sk = du.id_usuario_sk
        inner join {{ ref('dim_unidades') }} dun
            on fct.id_unidade_sk = dun.id_unidade_sk
        where fct.id_usuario_sk is not null
            and fct.descricao_evolucao is not null
            and fct.descricao_evolucao != ''

        union all

        -- Parte 2: Evolucoes familiares (com id_paciente_familia preenchido)
        -- A correcao da fct_evolucoes ja preenche id_paciente_familia
        -- para todas as evolucoes de origem familia. Nao ha mais registros
        -- orfaos, eliminando a necessidade de fallback via raw_membros_familia.
        select
            du.id_usuario as id_paciente,
            dun.id_unidade,
            fct.data_evolucao as data_interacao
        from {{ ref('fct_evolucoes') }} fct
        inner join {{ ref('dim_usuarios') }} du
            on fct.id_paciente_familia = du.id_usuario
        inner join {{ ref('dim_unidades') }} dun
            on fct.id_unidade_sk = dun.id_unidade_sk
        where fct.id_usuario_sk is null
            and fct.id_paciente_familia is not null
            and fct.descricao_evolucao is not null
            and fct.descricao_evolucao != ''
    ),

    atendimentos_por_pessoa_unidade as (
        select
            fct.id_usuario as id_paciente,
            fct.id_unidade,
            fct.data_atendimento as data_interacao
        from {{ ref('fct_atendimentos') }} fct
        where coalesce(fct.flag_cancelado, 'N') != 'S'
    ),

    interacoes_unificadas as (
        select
            id_paciente,
            id_unidade,
            data_interacao,
            'atendimento' as tipo_interacao
        from atendimentos_por_pessoa_unidade

        union all

        select
            id_paciente,
            id_unidade,
            data_interacao,
            'evolucao' as tipo_interacao
        from evolucoes_por_pessoa_unidade
    ),

    agregado_por_pessoa_unidade as (
        select
            id_paciente,
            id_unidade,
            logical_or(tipo_interacao = 'atendimento') as flag_atendimento,
            logical_or(tipo_interacao = 'evolucao') as flag_evolucao,
            countif(tipo_interacao = 'atendimento') as total_atendimentos,
            countif(tipo_interacao = 'evolucao') as total_evolucoes,
            min(data_interacao) as data_primeira_interacao,
            max(data_interacao) as data_ultima_interacao
        from interacoes_unificadas
        group by id_paciente, id_unidade
    ),

    familia_pessoa as (
        -- Familia atual da pessoa (prioriza vinculo ativo, depois o mais recente)
        select
            id_paciente,
            id_familia
        from (
            select
                id_paciente,
                id_familia,
                row_number() over (
                    partition by id_paciente
                    order by
                        case when data_saida is null then 0 else 1 end,
                        data_entrada desc
                ) as rn
            from {{ ref('raw_membros_familia') }}
        )
        where rn = 1
    ),

    usuario_enriquecido as (
        select
            u.id_usuario,
            u.nome,
            u.sexo,
            u.data_nascimento,
            date_diff(current_date(), u.data_nascimento, year) as idade,
            array(
                select violacao.descricao
                from unnest(coalesce(u.violacoes, [])) as violacao
                where violacao.descricao is not null
                order by violacao.codigo
            ) as violacoes_descricao
        from {{ ref('dim_usuarios') }} u
    ),

    final as (
        select
            a.id_paciente,
            fp.id_familia,
            ue.nome as nome_usuario,
            a.id_unidade,
            du.nome_unidade,
            du.nome_tipo as tipo_unidade,
            a.flag_atendimento,
            a.flag_evolucao,
            a.total_atendimentos,
            a.total_evolucoes,
            a.data_primeira_interacao,
            a.data_ultima_interacao,
            ue.sexo,
            ue.idade,
            ue.violacoes_descricao
        from agregado_por_pessoa_unidade a
        left join {{ ref('dim_unidades') }} du
            on a.id_unidade = du.id_unidade
        left join familia_pessoa fp
            on a.id_paciente = fp.id_paciente
        left join usuario_enriquecido ue
            on a.id_paciente = ue.id_usuario
    )

select * from final
