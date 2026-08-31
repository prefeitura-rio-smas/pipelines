-- Mart: Prontuário Carioca para PIC
-- Grão: 1 linha por família
--
-- Origens de identificação:
--   1. Projeto social PIC (id_projeto_social = 2) via dim_familias.projetos_sociais
--   2. Evolução aba Pequenos Cariocas (codigo_abrangencia = 20) via fct_evolucoes
--
-- Membros: crianças de 0 a 6 anos ativas na família
-- Indicadores propagam para toda a família (LOGICAL_OR)
-- Versão _dev para validação

{{ config(
    schema="pic",
    alias="mart_pic_prontuario_carioca",
    materialized="table"
) }}

with

-- ORIGEM 1: Projeto social PIC
familias_projeto_social as (
    select distinct f.id_familia
    from {{ ref('dim_familias') }} f,
    unnest(f.projetos_sociais) as p
    where p.id_projeto_social = 2
      and p.indicador_ativo = 'S'
      and p.data_cancelamento is null
),

-- ORIGEM 2: Evolução aba Pequenos Cariocas
familias_evolucao_aba20 as (
    select distinct e.id_familia
    from {{ ref('fct_evolucoes') }} e
    where e.codigo_abrangencia = 20
      and e.id_familia is not null
),

-- UNIÃO das duas origens com identificação
familias_uniao as (
    select id_familia, 'projeto_social' as origem
    from familias_projeto_social
    union all
    select id_familia, 'evolucao_aba_20' as origem
    from familias_evolucao_aba20
),

-- Agrega origens por família
familias_origens as (
    select
        id_familia,
        array_agg(distinct origem order by origem) as origem_identificacao
    from familias_uniao
    group by id_familia
),

-- Responsável familiar
responsavel_familiar as (
    select
        f.id_familia,
        struct(
            r.cpf as cpf,
            r.nome as nome
        ) as responsavel_familiar
    from {{ ref('dim_familias') }} f
    left join {{ ref('dim_usuarios') }} r
        on f.id_usuario_responsavel = r.id_usuario
),

-- Membros da família: crianças 0-6 ativas
membros_familia as (
    select
        m.id_familia,
        array_agg(
            struct(
                u.nome as nome,
                {{ calc_idade('u.data_nascimento') }} as idade,
                u.cpf as cpf
            )
            order by u.data_nascimento, u.nome
        ) as membros
    from {{ ref('raw_membros_familia') }} m
    inner join {{ ref('raw_usuarios') }} u
        on m.id_paciente = u.id_paciente
    where m.data_saida is null
      and u.cpf is not null
      and u.cpf != ''
      and upper(trim(u.nome)) not like 'TESTE%'
      and {{ calc_idade('u.data_nascimento') }} between 0 and 6
    group by m.id_familia
),

-- Indicador de violação de direito por família (LOGICAL_OR entre membros)
violacoes_indicador as (
    select
        m.id_familia,
        logical_or(du.flag_possui_violacao_direito = 'Sim') as indicador_violacao_direito
    from {{ ref('raw_membros_familia') }} m
    inner join {{ ref('dim_usuarios') }} du
        on m.id_paciente = du.id_usuario
    where m.data_saida is null
    group by m.id_familia
),

-- Descrições de violações por família (unnest dos arrays de cada membro)
violacoes_descricoes as (
    select
        m.id_familia,
        array_agg(distinct v.descricao ignore nulls) as violacao_direito
    from {{ ref('raw_membros_familia') }} m
    inner join {{ ref('dim_usuarios') }} du
        on m.id_paciente = du.id_usuario,
    unnest(du.violacoes) as v
    where
        m.data_saida is null
        and v.origem = 'cadastro'
    group by m.id_familia
),

-- Filiação documental (última evolução codigo_abrangencia=24 por família)
filiacao_familia as (
    {{ extrair_formulario(
        source_relation = ref('fct_evolucoes'),
        group_cols = ['id_familia'],
        codigo_abrangencia = 24,
        titulo_formulario = 'Documentação Civil',
        latest_by = 'data_evolucao',
        campos = [
            {'label': '%Há interesse%', 'col': 'interesse_filiacao_completa', 'type': 'exists'},
            {'label': '%Filiação completa%', 'col': 'possui_filiacao_completa', 'type': 'boolean', 'true_value': 'Sim'},
        ]
    ) }}
),

-- Encaminhamentos do formulário Documentação Civil (codigo_abrangencia=24)
documentacao_civil_encaminhamentos as (
    {{ extrair_formulario(
        source_relation = ref('fct_evolucoes'),
        group_cols = ['id_familia'],
        codigo_abrangencia = 24,
        titulo_formulario = 'Documentação Civil',
        campos = [
            {'label': '%Encaminhamentos%', 'col': 'encaminhamentos_documentacao_civil', 'type': 'array_agg'},
        ]
    ) }}
),

-- Busca Ativa Pequenos Cariocas (extrair_formulario: latest + parser HTML + pivot)
busca_ativa_pequenos_cariocas as (
    {{ extrair_formulario(
        source_relation = ref('fct_evolucoes'),
        group_cols = ['id_familia'],
        codigo_abrangencia = 20,
        titulo_formulario = '1. Pequenos Cariocas - Busca Ativa',
        latest_by = 'data_evolucao',
        flag_col = 'possui_busca_ativa_pequenos_cariocas',
        campos = [
            {'label': '%Protocolo violado%', 'col': 'protocolo_violado_busca_ativa', 'type': 'array'},
            {'label': '%Data em que a Busca Ativa%', 'col': 'data_busca_ativa', 'type': 'date', 'format': '%d/%m/%Y'},
            {'label': '%Tipo de busca ativa%', 'col': 'tipo_busca_ativa', 'type': 'array'},
            {'label': '%Família localizada%', 'col': 'familia_localizada_busca_ativa', 'type': 'boolean', 'true_value': 'Sim'},
            {'label': '%Se não, por quê%', 'col': 'motivo_nao_localizada_busca_ativa', 'type': 'array'},
        ]
    ) }}
),

-- Junção final
final as (
    select
        rf.responsavel_familiar,
        fo.origem_identificacao,
        coalesce(mf.membros, []) as membros,
        coalesce(vi.indicador_violacao_direito, false) as indicador_violacao_direito,
        coalesce(vd.violacao_direito, []) as violacao_direito,
        ff.possui_filiacao_completa,
        coalesce(ff.interesse_filiacao_completa, false) as interesse_filiacao_completa,
        coalesce(array_length(dce.encaminhamentos_documentacao_civil) > 0, false) as possui_encaminhamento_documentacao_civil,
        coalesce(dce.encaminhamentos_documentacao_civil, []) as encaminhamentos_documentacao_civil,
        coalesce(bapc.possui_busca_ativa_pequenos_cariocas, false) as possui_busca_ativa_pequenos_cariocas,
        coalesce(bapc.protocolo_violado_busca_ativa, []) as protocolo_violado_busca_ativa,
        bapc.data_busca_ativa as data_busca_ativa,
        coalesce(bapc.tipo_busca_ativa, []) as tipo_busca_ativa,
        coalesce(bapc.familia_localizada_busca_ativa, false) as familia_localizada_busca_ativa,
        coalesce(bapc.motivo_nao_localizada_busca_ativa, []) as motivo_nao_localizada_busca_ativa,
        {{ extrair_ultima_atualizacao('raw_configuracoes_sistema') }} as ultima_atualizacao
    from familias_origens fo
    left join responsavel_familiar rf on fo.id_familia = rf.id_familia
    left join membros_familia mf on fo.id_familia = mf.id_familia
    left join violacoes_indicador vi on fo.id_familia = vi.id_familia
    left join violacoes_descricoes vd on fo.id_familia = vd.id_familia
    left join filiacao_familia ff on fo.id_familia = ff.id_familia
    left join documentacao_civil_encaminhamentos dce on fo.id_familia = dce.id_familia
    left join busca_ativa_pequenos_cariocas bapc on fo.id_familia = bapc.id_familia
    where rf.responsavel_familiar.cpf is not null
)

select * from final
