{{ config(tags = ['planilha_unificada']) }}

with esperado as (
    select
        cast(p.id_usuario as string) as id_usuario,
        f.id_evolucao_modulo,
        row_number() over (partition by p.id_usuario, f.id_evolucao_modulo order by posicao) - 1 as posicao,
        to_json_string(struct(
            coalesce(f.titulo_formulario, '') as titulo_formulario,
            coalesce(f.observacoes, '') as observacoes,
            array(
                select concat(coalesce(c.label, ''), ': ', coalesce(c.valor, ''))
                from unnest(f.campos) as c with offset as ordem
                order by ordem
            ) as campos
        )) as formulario
    from {{ pu_ref('int_pu_formularios') }} as p, unnest(p.formularios) as f with offset as posicao
    inner join {{ pu_ref('dim_usuarios') }} as u on p.id_usuario = u.id_usuario
),

obtido as (
    select
        p.usuario.id_usuario,
        v.id_evolucao_modulo,
        posicao,
        to_json_string(f) as formulario
    from {{ ref('planilha_unificada') }} as p, unnest(p.evolucoes.registros) as v,
        unnest(v.formularios) as f with offset as posicao
)

select coalesce(e.id_usuario, o.id_usuario) as id_usuario
from esperado as e full outer join obtido as o on e.id_usuario = o.id_usuario and e.id_evolucao_modulo = o.id_evolucao_modulo and e.posicao = o.posicao
where e.formulario is distinct from o.formulario
