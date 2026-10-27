{% macro pu_nome_dimensao(servico) -%}
    {{ 'dim_outros_equipamentos' if servico == 'outros' else 'dim_' ~ servico }}
{%- endmacro %}

{% macro pu_tipo_servico(expressao) %}
    case {{ expressao }} when 'CRAS' then 'cras' when 'CREAS' then 'creas'
        when 'Centro POP' then 'centro_pop' else 'outros' end
{% endmacro %}

{% macro pu_ref(nome) %}
    {# Register the real dependency even when its input is redirected. #}
    {% set dependencia = ref(nome) %}
    {% if execute == true and nome in var('pu_fontes', {}) %}
        {{ api.Relation.create(database=target.database, schema=var('pu_base_schema'), identifier=nome) }}
    {% else %}
        {{ dependencia }}
    {% endif %}
{% endmacro %}

{% macro pu_dimensoes_sql() %}
    {% for servico in ['cras', 'creas', 'centro_pop', 'outros'] %}
        {% set nome = pu_nome_dimensao(servico) %}
        {% set dependencia = ref(nome) %}
        {% set relacao = var('pu_dimensoes', {}).get(servico, dependencia) if execute == true else dependencia %}
        select * from {{ relacao }}
        {% if not loop.last %}union all{% endif %}
    {% endfor %}
{% endmacro %}

{% macro pu_contagens_servico(registros) %}
    struct({% for servico in ['cras', 'creas', 'centro_pop', 'outros'] %}(select count(*) from unnest({{ registros }}) as r where r.tipo_servico = '{{ servico }}') as {{ servico }}{% if not loop.last %}, {% endif %}{% endfor %})
{% endmacro %}

{% macro pu_dimensao_sql(servico) %}
with atendimentos as (
    select p.id_usuario, array_agg(a order by a.data_atendimento, a.hora_atendimento,
        a.id_atendimento_modulo, a.id_unidade, a.id_tipo_atendimento,
        a.tipo_atendimento_descricao, a.flag_cancelado) as atendimentos
    from {{ pu_ref('int_pu_atendimentos') }} as p,
        unnest(p.atendimentos) as a
    inner join {{ pu_ref('dim_usuarios') }} as u on p.id_usuario = u.id_usuario
    where a.tipo_servico = '{{ servico }}'
    group by p.id_usuario
),
evolucoes_entrada as (
    select e.* from {{ pu_ref('int_pu_evolucoes') }} as e
    inner join {{ pu_ref('dim_usuarios') }} as u on e.id_usuario = u.id_usuario
    where e.tipo_servico = '{{ servico }}'
),
formularios_entrada as (
    {{ pu_formularios_sql('evolucoes_entrada') }}
),
formularios as (
    select p.id_usuario, f.id_evolucao_modulo,
        array_agg(struct(f.titulo_formulario, f.observacoes, f.campos) order by posicao) as formularios
    from formularios_entrada as p, unnest(p.formularios) as f with offset as posicao
    group by p.id_usuario, f.id_evolucao_modulo
),
evolucoes as (
    select e.id_usuario, array_agg((select as struct e.* except(id_usuario),
        coalesce(f.formularios, []) as formularios)
        order by e.data_evolucao, e.id_evolucao_modulo) as evolucoes
    from evolucoes_entrada as e
    left join formularios as f on e.id_usuario = f.id_usuario and e.id_evolucao_modulo = f.id_evolucao_modulo
    group by e.id_usuario
)
select coalesce(a.id_usuario, e.id_usuario) as id_usuario,
    coalesce(a.atendimentos, []) as atendimentos,
    coalesce(e.evolucoes, []) as evolucoes
from atendimentos as a full outer join evolucoes as e on a.id_usuario = e.id_usuario
{% endmacro %}

{% macro pu_esperado_sql(nome) %}
with dimensoes as ({{ pu_dimensoes_sql() }})
{% if nome == 'int_pu_atendimentos' %}
    select d.id_usuario, array_agg(a order by a.data_atendimento, a.hora_atendimento, a.id_atendimento_modulo,
        a.id_unidade, a.id_tipo_atendimento, a.tipo_atendimento_descricao, a.flag_cancelado) as atendimentos
    from dimensoes as d, unnest(d.atendimentos) as a group by d.id_usuario
{% elif nome == 'int_pu_evolucoes' %}
    select d.id_usuario, e.* except(formularios) from dimensoes as d, unnest(d.evolucoes) as e
{% else %}
    select d.id_usuario,
        array_agg(struct(e.id_evolucao_modulo, f.titulo_formulario, f.observacoes, f.campos)
            order by e.data_evolucao, e.id_evolucao_modulo, posicao) as formularios
    from dimensoes as d, unnest(d.evolucoes) as e,
        unnest(e.formularios) as f with offset as posicao group by d.id_usuario
{% endif %}
{% endmacro %}

{% macro pu_contagens_diretas(registros) %}
    {% for servico in ['cras', 'creas', 'centro_pop', 'outros'] %}
        {% if not loop.first %},{% endif %}
        (select count(*) from unnest({{ registros }}) as evento where evento.tipo_servico = '{{ servico }}') as quantidade_{{ servico }}
    {% endfor %}
{% endmacro %}
