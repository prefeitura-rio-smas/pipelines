-- Mart: Prontuário Carioca para PIC
-- Grão: 1 linha por CPF de participante PIC
--
-- Cadeia Medallion:
--   dim_familias (projetos_sociais) → filtra PIC
--   raw_membros_familia → parentesco
--   raw_usuarios → dados pessoais
--   dim_usuarios → violações
--   fct_evolucoes (codigo_abrangencia=24) → filiação documental

{{ config(schema="pic", materialized="table") }}

with

-- Famílias PIC via dim_familias enriquecida
familias_pic as (
    select
        f.id_familia,
        f.id_usuario_responsavel,
        f.nome_responsavel
    from {{ ref('dim_familias') }} f,
    unnest(f.projetos_sociais) as p
    where p.id_projeto_social = 2
      and p.indicador_ativo = 'S'
      and p.data_cancelamento is null
),

-- Membros ativos dessas famílias (último vínculo por pessoa)
membros_pic as (
    select
        m.id_paciente,
        m.parentesco_responsavel as parentesco,
        f.id_usuario_responsavel,
        f.nome_responsavel
    from {{ ref('raw_membros_familia') }} m
    inner join familias_pic f on m.id_familia = f.id_familia
    where m.data_saida is null
    qualify
        row_number() over (
            partition by m.id_paciente
            order by m.data_entrada desc
        ) = 1
),

-- Dados pessoais dos participantes PIC
participantes as (
    select
        u.id_paciente as id_usuario,
        u.cpf,
        u.nome,
        mp.parentesco,
        mp.id_usuario_responsavel
    from {{ ref('raw_usuarios') }} u
    inner join membros_pic mp on u.id_paciente = mp.id_paciente
    where u.cpf is not null
      and upper(trim(u.nome)) not like 'TESTE%'
),

-- Dados do responsável familiar (via dim_usuarios)
responsavel as (
    select id_usuario, cpf, nome
    from {{ ref('dim_usuarios') }}
),

-- Violações (já consolidada em dim_usuarios)
violacoes as (
    select
        id_usuario,
        cpf,
        nome,
        flag_possui_violacao_direito,
        violacoes
    from {{ ref('dim_usuarios') }}
    where cpf is not null
),

-- Filiação via fct_evolucoes (agora com codigo_abrangencia)
filiacao_documental as (
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

-- Junção final com ordenação exata
final as (
    select
        struct(
            r.cpf as cpf,
            r.nome as nome
        ) as responsavel_familiar,
        pp.cpf,
        pp.nome,
        pp.parentesco,
        case
            when v.flag_possui_violacao_direito = 'Sim' then true
            else false
        end as indicador_violacao_direito,
        array(
            select descricao from unnest(v.violacoes)
        ) as violacao_direito,
        coalesce(fd.possui_filiacao_completa, false) as possui_filiacao_completa,
        coalesce(fd.interesse_filiacao_completa, false) as interesse_filiacao_completa
    from participantes pp
    left join responsavel r
        on pp.id_usuario_responsavel = r.id_usuario
    left join violacoes v
        on pp.id_usuario = v.id_usuario
    left join filiacao_documental fd
        on pp.id_usuario = fd.id_paciente
)

select * from final
