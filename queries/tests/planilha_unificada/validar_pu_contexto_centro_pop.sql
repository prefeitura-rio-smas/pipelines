{{ config(tags=['planilha_unificada']) }}
with esperado as (
    select
        cast(u.id_usuario as string) as id_usuario,
        to_json_string(struct({{ pu_cpop_perfil_publico('d') }})) as perfil
    from {{ pu_ref('dim_usuarios') }} as u
    left join {{ ref('dim_centro_pop') }} as d on u.id_usuario = d.id_usuario
),

obtido as (
    select
        usuario.id_usuario,
        to_json_string((select as struct escopos.centro_pop.* except (historico, participacoes))) as perfil
    from {{ ref('planilha_unificada') }}
)

select coalesce(e.id_usuario, o.id_usuario) as id_usuario
from esperado as e full outer join obtido as o on e.id_usuario = o.id_usuario
where e.perfil is distinct from o.perfil
union all
select id_usuario from obtido
group by id_usuario
having count(*) != 1 or id_usuario is null
