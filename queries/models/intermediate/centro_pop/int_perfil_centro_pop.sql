{{ config(materialized='ephemeral', enabled=target.name != 'prod', tags=['planilha_unificada']) }}
-- Commas precede dynamically generated Jinja projections.
-- noqa: disable=LT04
with ultimos as (
    -- Selecionar a ocorrência inteira antes de projetar respostas, inclusive vazias.
    select * from {{ pu_ref('int_registros_centro_pop') }}
    where tipo_registro in ({% for assunto in pu_cpop_perfil_assuntos() %}'{{ assunto }}'{% if not loop.last %}, {% endif %}{% endfor %})
    qualify row_number() over (
        partition by id_usuario, tipo_registro
        order by data_registro desc nulls last, data_resposta desc nulls last, id_registro desc
    ) = 1
),

unidades_atendimento as (
    select
        p.id_usuario,
        array_agg(
            distinct nullif(trim(a.nome_unidade), '') ignore nulls
            order by nullif(trim(a.nome_unidade), '')
        ) as unidades_atendimento
    from {{ pu_ref('int_atendimentos_por_usuario') }} as p, unnest(p.atendimentos) as a
    inner join {{ pu_ref('dim_usuarios') }} as u on p.id_usuario = u.id_usuario
    where a.tipo_servico = 'centro_pop'
    group by p.id_usuario
),

usuarios as (
    select id_usuario from ultimos
    union distinct
    select id_usuario from unidades_atendimento
)

select
    u.id_usuario,
    coalesce(unidades.unidades_atendimento, []) as unidades_atendimento,
    if(pai.id_registro is not null, 'Sim', '') as possui_registro_pai
    {% for assunto, campos in pu_cpop_perfil_assuntos().items() %}
        , {{ assunto }}.data_registro as data_registro_{{ assunto }},
        {{ assunto }}.data_resposta as data_resposta_{{ assunto }}
        {% for campo in campos %}
            , {% if campo == 'encaminhamentos' %}
                array(
                    select distinct concat(e.label, ': ', e.valor) from unnest({{ assunto }}.encaminhamentos) as e
                    where nullif(trim(e.label), '') is not null and nullif(trim(e.valor), '') is not null
                    order by 1
                )
            {% elif campo == 'demandas' %}coalesce({{ assunto }}.{{ campo }}, [])
            {% elif pu_cpop_campos()[campo] == 'DATE' %}{{ assunto }}.{{ campo }}
            {% else %}coalesce({{ assunto }}.{{ campo }}, ''){% endif %} as {{ pu_cpop_perfil_nome(assunto, campo) }}
        {% endfor %}
    {% endfor %}
from usuarios as u
left join unidades_atendimento as unidades on u.id_usuario = unidades.id_usuario
{% for assunto in pu_cpop_perfil_assuntos() %}
    left join ultimos as {{ assunto }} on u.id_usuario = {{ assunto }}.id_usuario and {{ assunto }}.tipo_registro = '{{ assunto }}'
{% endfor %}
