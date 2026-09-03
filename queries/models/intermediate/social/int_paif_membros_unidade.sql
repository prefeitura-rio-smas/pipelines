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
        coalesce(p.id_unidade, ul.id_unidade) as id_unidade
    from paif as p
    inner join membros as m on p.id_familia = m.id_familia
    inner join usuarios as u on m.id_usuario = u.id_usuario
    left join vulnerabilidades_familia as vf on p.id_familia = vf.id_familia
    left join unidade_por_login as ul on p.id_login_cadastro = ul.id_login
)

select * from final
