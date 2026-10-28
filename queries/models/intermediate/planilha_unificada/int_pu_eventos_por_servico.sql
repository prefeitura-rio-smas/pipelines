with atendimentos as (
    select
        p.id_usuario,
        a.tipo_servico,
        array_agg(
            a order by
                a.data_atendimento, a.hora_atendimento,
                a.id_atendimento_modulo, a.id_unidade, a.id_tipo_atendimento,
                a.tipo_atendimento_descricao, a.flag_cancelado
        ) as atendimentos
    from {{ pu_ref('int_pu_atendimentos') }} as p,
        unnest(p.atendimentos) as a
    inner join {{ pu_ref('dim_usuarios') }} as u on p.id_usuario = u.id_usuario
    group by p.id_usuario, a.tipo_servico
),

evolucoes_entrada as (
    select e.* from {{ pu_ref('int_pu_evolucoes') }} as e
    inner join {{ pu_ref('dim_usuarios') }} as u on e.id_usuario = u.id_usuario
),

formularios_entrada as (
    select * from {{ pu_ref('int_pu_formularios') }}
),

formularios as (
    select
        p.id_usuario,
        f.id_evolucao_modulo,
        array_agg(struct(f.titulo_formulario, f.observacoes, f.campos) order by posicao) as formularios
    from formularios_entrada as p, unnest(p.formularios) as f with offset as posicao
    group by p.id_usuario, f.id_evolucao_modulo
),

evolucoes as (
    select
        e.id_usuario,
        e.tipo_servico,
        array_agg((
            select as struct
                e.* except (id_usuario),
                coalesce(f.formularios, []) as formularios
        )
        order by e.data_evolucao, e.id_evolucao_modulo) as evolucoes
    from evolucoes_entrada as e
    left join formularios as f on e.id_usuario = f.id_usuario and e.id_evolucao_modulo = f.id_evolucao_modulo
    group by e.id_usuario, e.tipo_servico
)

select
    coalesce(a.id_usuario, e.id_usuario) as id_usuario,
    coalesce(a.tipo_servico, e.tipo_servico) as tipo_servico,
    coalesce(a.atendimentos, []) as atendimentos,
    coalesce(e.evolucoes, []) as evolucoes
from atendimentos as a full outer join evolucoes as e on a.id_usuario = e.id_usuario and a.tipo_servico = e.tipo_servico
