{{ config(materialized = 'ephemeral') }}

with paif as (
    select
        id_familia,
        id_unidade,
        id_login_cadastro,
        data_cadastro as data_cadastro_paif
    from {{ ref('raw_familias_servicos_assistenciais') }}
    where
        id_servico_assistencial = 1
        and data_cancelamento is null
),

unidade_por_login as (
    select
        id_login,
        array_agg(id_unidade order by id_unidade limit 1)[offset(0)] as id_unidade
    from {{ ref('raw_operadores_unidades') }}
    group by id_login
),

unidade_por_atendimento as (
    -- Terceiro nível de atribuição: unidade CRAS do atendimento mais recente
    -- da família (atividade real de serviço). Restrito a CRAS para não atribuir
    -- CREAS/outras à família no RMA CRAS.
    select
        a.id_familia,
        array_agg(a.id_unidade order by a.data_atendimento desc, a.id_unidade asc limit 1)[offset(0)] as id_unidade
    from {{ ref('raw_atendimentos_familias') }} as a
    inner join {{ ref('dim_unidades') }} as d
        on
            a.id_unidade = d.id_unidade
            and d.tipo_unidade = 'CRAS'
    where coalesce(a.flag_cancelado, 'N') != 'S'
    group by a.id_familia
),

membros as (
    select
        id_familia,
        id_paciente as id_usuario
    from {{ ref('raw_membros_familia') }}
    where data_saida is null
),

usuarios as (
    select
        id_usuario,
        data_nascimento,
        beneficio,
        violacoes
    from {{ ref('dim_usuarios') }}
),

vulnerabilidades_familia as (
    select
        id_familia,
        array_agg(
            struct(
                id_vulnerabilidade,
                data_cadastro
            )
        ) as vulnerabilidades
    from {{ ref('raw_familias_vulnerabilidades') }}
    where data_cancelamento is null
    group by id_familia
),

final as (
    select
        p.id_familia,
        p.data_cadastro_paif,
        m.id_usuario,
        u.data_nascimento,
        u.beneficio,
        u.violacoes,
        vf.vulnerabilidades,
        coalesce(p.id_unidade, ul.id_unidade, af.id_unidade) as id_unidade
    from paif as p
    inner join membros as m on p.id_familia = m.id_familia
    inner join usuarios as u on m.id_usuario = u.id_usuario
    left join vulnerabilidades_familia as vf on p.id_familia = vf.id_familia
    left join unidade_por_login as ul on p.id_login_cadastro = ul.id_login
    left join unidade_por_atendimento as af on p.id_familia = af.id_familia
)

select * from final
