-- Mart: Prontuário Carioca para PIC
-- Grão: 1 linha por família
--
-- Origens de identificação:
--   1. Projeto social PIC (id_projeto_social = 2) via dim_familias.projetos_sociais
--   2. Evolução aba Pequenos Cariocas (codigo_abrangencia = 20) via fct_evolucoes
--
-- Membros: crianças de 0 a 6 anos ativas na família
-- Indicadores propagam para toda a família (LOGICAL_OR)

{{ config(schema="pic", materialized="table") }}

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
                date_diff(current_date(), u.data_nascimento, year) as idade,
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
      and date_diff(current_date(), u.data_nascimento, year) between 0 and 6
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
    where m.data_saida is null
    group by m.id_familia
),

-- Filiação documental (última evolução codigo_abrangencia=24 por pessoa)
filiacao_por_pessoa as (
    select
        dim_u.id_usuario as id_paciente,
        regexp_contains(
            e.descricao_evolucao,
            r'Filiação completa na certidão de nascimento\?.*?<b>\s*Sim\s*</b>'
        ) as possui_filiacao_completa,
        regexp_contains(
            e.descricao_evolucao,
            r'Há interesse em tomar as medidas necessárias para inclusão da filiação faltante\?.*?<b>\s*Sim\s*</b>'
        ) as interesse_filiacao_completa
    from {{ ref('fct_evolucoes') }} e
    inner join {{ ref('dim_usuarios') }} dim_u
        on e.id_usuario_sk = dim_u.id_usuario_sk
    where e.codigo_abrangencia = 24
    qualify
        row_number() over (
            partition by dim_u.id_usuario
            order by e.data_evolucao desc
        ) = 1
),

-- Sobe filiação para família
filiacao_familia as (
    select
        m.id_familia,
        logical_or(coalesce(fp.possui_filiacao_completa, false)) as possui_filiacao_completa,
        logical_or(coalesce(fp.interesse_filiacao_completa, false)) as interesse_filiacao_completa
    from {{ ref('raw_membros_familia') }} m
    left join filiacao_por_pessoa fp
        on m.id_paciente = fp.id_paciente
    where m.data_saida is null
    group by m.id_familia
),

-- Junção final
final as (
    select
        rf.responsavel_familiar,
        fo.origem_identificacao,
        coalesce(mf.membros, []) as membros,
        coalesce(vi.indicador_violacao_direito, false) as indicador_violacao_direito,
        coalesce(vd.violacao_direito, []) as violacao_direito,
        coalesce(ff.possui_filiacao_completa, false) as possui_filiacao_completa,
        coalesce(ff.interesse_filiacao_completa, false) as interesse_filiacao_completa
    from familias_origens fo
    left join responsavel_familiar rf on fo.id_familia = rf.id_familia
    left join membros_familia mf on fo.id_familia = mf.id_familia
    left join violacoes_indicador vi on fo.id_familia = vi.id_familia
    left join violacoes_descricoes vd on fo.id_familia = vd.id_familia
    left join filiacao_familia ff on fo.id_familia = ff.id_familia
)

select * from final
