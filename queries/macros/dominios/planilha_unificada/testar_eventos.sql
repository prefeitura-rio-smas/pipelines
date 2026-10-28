{% test pu_preservacao_eventos(model) %}
-- Independent conservation check: full internal event payloads against frozen inputs.
with esperado as (
    select cast(p.id_usuario as string) as id_usuario, 'atendimento' as bloco,
        a.id_atendimento_modulo as chave, to_json_string(a) as registro
    from {{ pu_ref('int_pu_atendimentos') }} as p, unnest(p.atendimentos) as a
    inner join {{ pu_ref('dim_usuarios') }} as u on p.id_usuario = u.id_usuario
    union all
    select cast(e.id_usuario as string), 'evolucao', e.id_evolucao_modulo,
        to_json_string((select as struct e.* except(id_usuario)))
    from {{ pu_ref('int_pu_evolucoes') }} as e
    inner join {{ pu_ref('dim_usuarios') }} as u on e.id_usuario = u.id_usuario
),
obtido as (
    select cast(d.id_usuario as string) as id_usuario, 'atendimento' as bloco,
        a.id_atendimento_modulo as chave, to_json_string(a) as registro
    from {{ model }} as d, unnest(d.atendimentos) as a
    union all
    select cast(d.id_usuario as string), 'evolucao', e.id_evolucao_modulo,
        to_json_string((select as struct e.* except(formularios)))
    from {{ model }} as d, unnest(d.evolucoes) as e
)
select coalesce(e.id_usuario, o.id_usuario) as id_usuario,
    coalesce(e.chave, o.chave) as chave
from esperado as e full outer join obtido as o using (id_usuario, bloco, chave)
where e.registro is distinct from o.registro
union all
select cast(id_usuario as string), 'duplicado_ou_vazio'
from {{ model }} as d
where id_usuario is null or (array_length(atendimentos) = 0 and array_length(evolucoes) = 0)
    or array_length(atendimentos) != (select count(distinct a.id_atendimento_modulo) from unnest(d.atendimentos) as a)
    or array_length(evolucoes) != (select count(distinct e.id_evolucao_modulo) from unnest(d.evolucoes) as e)
{% endtest %}

{% test pu_formularios_eventos(model) %}
with esperado as (
    select cast(p.id_usuario as string) as id_usuario, f.id_evolucao_modulo,
        row_number() over(partition by p.id_usuario,f.id_evolucao_modulo order by posicao)-1 as posicao,
        to_json_string(struct(f.titulo_formulario, f.observacoes, f.campos)) as formulario
    from {{ pu_ref('int_pu_formularios') }} as p,
        unnest(p.formularios) as f with offset as posicao
    inner join {{ pu_ref('dim_usuarios') }} as u on p.id_usuario = u.id_usuario
    inner join {{ pu_ref('int_pu_evolucoes') }} as e
        on p.id_usuario=e.id_usuario and f.id_evolucao_modulo=e.id_evolucao_modulo
), obtido as (
    select cast(d.id_usuario as string) as id_usuario, e.id_evolucao_modulo, posicao,
        to_json_string(f) as formulario
    from {{ model }} as d, unnest(d.evolucoes) as e,
        unnest(e.formularios) as f with offset as posicao
)
select coalesce(e.id_usuario,o.id_usuario) as id_usuario
from esperado e full outer join obtido o using(id_usuario,id_evolucao_modulo,posicao)
where e.formulario is distinct from o.formulario
{% endtest %}
