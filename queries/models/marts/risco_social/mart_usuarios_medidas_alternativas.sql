-- Mart: Usuários em Penas e Medidas Alternativas (PMA)
-- Grão: 1 linha por pessoa (id_paciente). Base do relatório 'PMA por CREAS'.
--
-- Origens (array origem_identificacao):
--   1. indicador_projeto_social: projeto social PMA (id_projeto_social = 23, ativo, sem
--      cancelamento) no prontuário do usuário (dim_usuarios.projetos_sociais — array já
--      expandido da família para os membros ativos na própria dimensão).
--   2. formulario_pma: evolução não cancelada com codigo_abrangencia = 18 e formulário
--      'CREAS - Penas e Medidas Alternativas' (módulos família e usuário). Campos extraídos
--      via macro extrair_formulario (última evolução por pessoa, latest_by = data_evolucao).
--
-- Vínculo ao CREAS — Cenário A (fallback para vazios, validado com o Leone):
--   id_unidade_vinculo = COALESCE(unidade_cadastro_marcacao, unidade_referencia,
--   unidade_cadastro_operador):
--     marcação: unidade que cadastrou a marcação (última evolução do formulário, senão a do
--       indicador do projeto 23); referência: id_unidade_referencia (dim_usuarios);
--       operador: unidade do operador cadastrante (dim_profissionais.ids_unidade[0]).
--   vinculado_creas = (id_unidade_vinculo é CREAS em dim_unidades, id_tipo_unidade = 13).
--   Unidades de teste (nome contendo 'TESTE') são excluídas por design da dim_unidades: não são
--   CREAS nem aparecem em nome_unidade_vinculo. COALESCE puro, sem flags de teste.

{{ config(materialized='table', tags=['risco_social']) }}

with

-- Base: usuários únicos da dimensão (robustez contra duplicidade de linha por usuário)
usuarios_unicos as (
    select
        id_usuario,
        any_value(id_unidade_referencia) as id_unidade_referencia,
        any_value(id_login_cadastro) as id_login_cadastro,
        array_concat_agg(projetos_sociais) as projetos_sociais
    from {{ ref('dim_usuarios') }}
    group by id_usuario
),

-- ORIGEM 1: indicador do projeto social 23 (ativo, sem cancelamento) -> 1 linha por pessoa
indicador_pma as (
    select
        id_usuario as id_paciente,
        min(p.id_unidade) as id_unidade_cadastro
    from usuarios_unicos,
    unnest(projetos_sociais) as p
    where p.id_projeto_social = 23
      and p.indicador_ativo = 'S'
      and p.data_cancelamento is null
    group by id_usuario
),

-- ORIGEM 2: evoluções do formulário PMA (família + usuário), não canceladas
evolucoes_pma as (
    select
        coalesce(du.id_usuario, e.id_paciente_familia) as id_paciente,
        e.id_unidade as id_unidade_cadastro,
        e.data_evolucao,
        e.descricao_evolucao,
        e.codigo_abrangencia,
        e.origem_modulo,
        e.id_familia
    from {{ ref('fct_evolucoes') }} e
    left join {{ ref('dim_usuarios') }} du
        on e.id_usuario_sk = du.id_usuario_sk
    where e.codigo_abrangencia = 18
      and e.data_cancelamento is null
      and e.origem_modulo in ('familia', 'usuario')
      and coalesce(du.id_usuario, e.id_paciente_familia) is not null
),

-- Campos do formulário PMA (última evolução por pessoa)
formulario_pma as (
    {{ extrair_formulario(
        source_relation = 'evolucoes_pma',
        group_cols = ['id_paciente'],
        codigo_abrangencia = 18,
        titulo_formulario = 'CREAS - Penas e Medidas Alternativas',
        latest_by = 'data_evolucao',
        campos = [
            {'label': '%Data em que o usuário se apresentou%', 'col': 'data_apresentacao', 'type': 'date', 'format': '%d/%m/%Y'},
            {'label': '%Data de início da prestação de serviços%', 'col': 'data_inicio_servico', 'type': 'date', 'format': '%d/%m/%Y'},
            {'label': '%Local onde está cumprindo a pena%', 'col': 'local_cumprimento_pena'},
            {'label': '%Data do desligamento%', 'col': 'data_desligamento', 'type': 'date', 'format': '%d/%m/%Y'},
            {'label': '%Motivo do desligamento%', 'col': 'motivo_desligamento'},
            {'label': '%Tempo de cumprimento da pena alternativa%', 'col': 'tempo_cumprimento_pena'},
        ]
    ) }}
),

-- Unidade de cadastro, família e data da última evolução PMA por pessoa (max_by)
ultima_evolucao_pma as (
    select
        id_paciente,
        max(data_evolucao) as data_ultima_evolucao_formulario,
        max_by(id_unidade_cadastro, data_evolucao) as id_unidade_cadastro,
        max_by(id_familia, data_evolucao) as id_familia
    from evolucoes_pma
    group by id_paciente
),

-- União das origens com identificação por pessoa
pessoas_origens as (
    select
        id_paciente,
        array_agg(distinct origem order by origem) as origem_identificacao
    from (
        select distinct id_paciente, 'indicador_projeto_social' as origem from indicador_pma
        union all
        select distinct id_paciente, 'formulario_pma' as origem from evolucoes_pma
    )
    group by id_paciente
),

-- FALLBACK nível 2: unidade do operador cadastrante (via dim_profissionais)
referencia_operador as (
    select
        uu.id_usuario as id_paciente,
        uu.id_unidade_referencia,
        dp.ids_unidade[safe_offset(0)] as id_unidade_cadastro_operador
    from usuarios_unicos uu
    left join {{ ref('dim_profissionais') }} dp
        on uu.id_login_cadastro = dp.id_login
),

-- Vínculo efetivo (Cenário A): COALESCE(marcação, referência, operador) puro
pessoa_vinculo as (
    select
        po.id_paciente,
        coalesce(ue.id_unidade_cadastro, i.id_unidade_cadastro) as id_unidade_cadastro_marcacao,
        ro.id_unidade_referencia,
        ro.id_unidade_cadastro_operador,
        case
            when coalesce(ue.id_unidade_cadastro, i.id_unidade_cadastro) is not null then 'unidade_cadastro_marcacao'
            when ro.id_unidade_referencia is not null then 'unidade_referencia'
            when ro.id_unidade_cadastro_operador is not null then 'unidade_cadastro_operador'
            else 'sem_unidade'
        end as origem_unidade_vinculo
    from pessoas_origens po
    left join ultima_evolucao_pma ue on po.id_paciente = ue.id_paciente
    left join indicador_pma i on po.id_paciente = i.id_paciente
    left join referencia_operador ro on po.id_paciente = ro.id_paciente
),

final as (
    select
        pv.id_paciente,
        ue.id_familia,
        po.origem_identificacao,
        (i.id_paciente is not null) as flag_indicador_projeto23,
        (ue.id_paciente is not null) as flag_formulario_pma,
        coalesce(
            pv.id_unidade_cadastro_marcacao,
            pv.id_unidade_referencia,
            pv.id_unidade_cadastro_operador
        ) as id_unidade_vinculo,
        pv.origem_unidade_vinculo,
        duv.nome_unidade as nome_unidade_vinculo,
        (duv.id_tipo_unidade = 13) as vinculado_creas,
        pv.id_unidade_referencia,
        dur.nome_unidade as nome_unidade_referencia,
        pv.id_unidade_cadastro_operador,
        duo.nome_unidade as nome_unidade_cadastro_operador,
        fpm.data_apresentacao,
        fpm.data_inicio_servico,
        fpm.local_cumprimento_pena,
        fpm.data_desligamento,
        fpm.motivo_desligamento,
        fpm.tempo_cumprimento_pena,
        ue.data_ultima_evolucao_formulario,
        {{ extrair_ultima_atualizacao('raw_configuracoes_sistema') }} as ultima_atualizacao
    from pessoa_vinculo pv
    left join pessoas_origens po on pv.id_paciente = po.id_paciente
    left join ultima_evolucao_pma ue on pv.id_paciente = ue.id_paciente
    left join indicador_pma i on pv.id_paciente = i.id_paciente
    left join {{ ref('dim_unidades') }} duv
        on coalesce(
            pv.id_unidade_cadastro_marcacao,
            pv.id_unidade_referencia,
            pv.id_unidade_cadastro_operador
        ) = duv.id_unidade
    left join {{ ref('dim_unidades') }} dur on pv.id_unidade_referencia = dur.id_unidade
    left join {{ ref('dim_unidades') }} duo on pv.id_unidade_cadastro_operador = duo.id_unidade
    left join formulario_pma fpm on pv.id_paciente = fpm.id_paciente
)

select * from final
