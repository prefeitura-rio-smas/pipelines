{% test pu_preservacao_eventos(model) %}
-- Confere identidade e multiplicidade dos eventos na saída pública.
with esperado as (
    select cast(p.id_usuario as string) as id_usuario, 'atendimento' as bloco,
        a.id_atendimento_modulo as chave
    from {{ pu_ref('int_atendimentos_por_usuario') }} as p, unnest(p.atendimentos) as a
    inner join {{ pu_ref('dim_usuarios') }} as u on p.id_usuario = u.id_usuario
    union all
    select cast(e.id_usuario as string), 'evolucao', e.id_evolucao_modulo
    from {{ pu_ref('int_evolucoes_por_usuario') }} as e
    inner join {{ pu_ref('dim_usuarios') }} as u on e.id_usuario = u.id_usuario
),
obtido as (
    select d.usuario.id_usuario, 'atendimento' as bloco, a.id_atendimento_modulo as chave
    from {{ model }} as d, unnest(d.atendimentos.registros) as a
    union all
    select d.usuario.id_usuario, 'evolucao', e.id_evolucao_modulo
    from {{ model }} as d, unnest(d.evolucoes.registros) as e
), esperado_contagem as (
    select id_usuario, bloco, chave, count(*) as quantidade from esperado group by 1, 2, 3
), obtido_contagem as (
    select id_usuario, bloco, chave, count(*) as quantidade from obtido group by 1, 2, 3
)
select coalesce(e.id_usuario, o.id_usuario) as id_usuario
from esperado_contagem e full outer join obtido_contagem o using (id_usuario, bloco, chave)
where e.quantidade is distinct from o.quantidade
{% endtest %}

{% test pu_formularios_eventos(model) %}
with esperado as (
    select cast(p.id_usuario as string) as id_usuario, f.id_evolucao_modulo,
        row_number() over(partition by p.id_usuario,f.id_evolucao_modulo order by posicao)-1 as posicao,
        to_json_string(struct(coalesce(f.titulo_formulario, '') as titulo_formulario,
            coalesce(f.observacoes, '') as observacoes,
            array(select concat(coalesce(c.label, ''), ': ', coalesce(c.valor, '')) from unnest(f.campos) c with offset ordem order by ordem) as campos)) as formulario
    from {{ pu_ref('int_formularios_por_usuario') }} as p,
        unnest(p.formularios) as f with offset as posicao
    inner join {{ pu_ref('dim_usuarios') }} as u on p.id_usuario = u.id_usuario
    inner join {{ pu_ref('int_evolucoes_por_usuario') }} as e
        on p.id_usuario=e.id_usuario and f.id_evolucao_modulo=e.id_evolucao_modulo
), obtido as (
    select d.usuario.id_usuario as id_usuario, e.id_evolucao_modulo, posicao,
        to_json_string(struct(f.titulo_formulario, f.observacoes, f.campos)) as formulario
    from {{ model }} as d, unnest(d.evolucoes.registros) as e,
        unnest(e.formularios) as f with offset as posicao
)
select coalesce(e.id_usuario,o.id_usuario) as id_usuario
from esperado e full outer join obtido o using(id_usuario,id_evolucao_modulo,posicao)
where e.formulario is distinct from o.formulario
{% endtest %}
