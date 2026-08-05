{{ config(materialized="ephemeral") }}

-- Projetos sociais do usuário (via famílias ativas).
-- Ponte família → usuário: expande os projetos das famílias ativas (data_saida is null)
-- para os membros, agrega por usuário e deduplica por projeto (mantém o cadastro mais
-- recente). Garante 1 linha por usuário mesmo com múltiplos vínculos familiares ativos.
with membros_ativos as (
    select
        id_paciente,
        id_familia
    from {{ ref('raw_membros_familia') }}
    where data_saida is null
),

projetos as (
    select
        m.id_paciente as id_usuario,
        array_concat_agg(fp.projetos_sociais) as projetos_sociais
    from membros_ativos m
    left join {{ ref('int_familias_projetos_sociais') }} fp
        on m.id_familia = fp.id_familia
    group by m.id_paciente
),

final as (
    select
        id_usuario,
        array_agg(proj) as projetos_sociais
    from (
        select
            id_usuario,
            proj,
            row_number() over (
                partition by id_usuario, proj.id_projeto_social
                order by proj.data_cadastro desc nulls last
            ) as rn
        from projetos,
        unnest(projetos_sociais) as proj
    )
    where rn = 1
    group by id_usuario
)

select * from final
