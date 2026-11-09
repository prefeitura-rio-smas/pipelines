with base as (
    select * from {{ ref('raw_familias') }}
),

responsavel as (
    select
        id_usuario,
        nome
    from {{ ref('int_usuarios') }}
),

membros as (
    select
        m.id_familia,
        u.id_usuario,
        m.data_entrada,
        row_number() over (
            partition by u.id_usuario
            order by m.data_entrada desc, m.id_familia desc
        ) = 1 as is_atual
    from {{ ref('raw_membros_familia') }} as m
    inner join {{ ref('int_usuarios') }} as u
        on m.id_paciente = u.id_usuario
    where m.data_saida is null
),

membros_por_familia as (
    select
        id_familia,
        array_agg(
            struct(
                id_usuario,
                data_entrada,
                is_atual
            )
            order by id_usuario
        ) as membros
    from membros
    group by id_familia
),

vulnerabilidades as (
    select * from {{ ref('int_vulnerabilidades_agregadas') }}
),

servicos as (
    select * from {{ ref('int_servicos_agregados') }}
),

projetos as (
    select * from {{ ref('int_familias_projetos_sociais') }}
),

final as (
    select
        base.id_familia,
        base.id_usuario_responsavel,
        r.nome as nome_responsavel,
        base.data_ultima_modificacao,
        base.flag_ativo,
        v.vulnerabilidades,
        s.servicos,
        p.projetos_sociais,
        m.membros
    from base
    inner join responsavel as r on base.id_usuario_responsavel = r.id_usuario
    left join vulnerabilidades as v on base.id_familia = v.id_familia
    left join servicos as s on base.id_familia = s.id_familia
    left join projetos as p on base.id_familia = p.id_familia
    left join membros_por_familia as m on base.id_familia = m.id_familia
)

select * from final
