{% test pu_cpop_integridade(model) %}
select id_registro
from {{ model }}
where conflitos != 0 or id_usuario is null or id_registro is null
    or tipo_registro not in ('pai', 'atendimento_social', 'saude', 'situacao_rua', 'desligamento', 'participacao')
    or abrangencia not in ('pessoa', 'unidade')
    or (tipo_registro = 'participacao' and (id_atividade is null or id_unidade is null or data_presenca is null))
    {% for campo in ['possui_referencias_familiares', 'possibilidade_reinsercao_familiar', 'uso_substancias_psicoativas', 'possui_problema_saude', 'realiza_acompanhamento_saude'] %}
        or ({{ campo }} is not null and {{ campo }} not in ('Sim', 'Não'))
    {% endfor %}
{% endtest %}

{% test pu_cpop_perfil(model) %}
with esperado as (select * from {{ ref('int_perfil_centro_pop') }}),
comparacao as (
    select id_usuario, to_json_string(e) valor, 'esperado' origem from esperado e
    union all
    select id_usuario, to_json_string(o) valor, 'obtido' origem from {{ model }} o
)
select id_usuario from comparacao group by id_usuario, valor
having countif(origem = 'esperado') != countif(origem = 'obtido')
{% endtest %}

{% test pu_cpop_sem_structs(model) %}
select column_name from {{ model.database }}.{{ model.schema }}.INFORMATION_SCHEMA.COLUMNS
where table_name = '{{ model.identifier }}' and regexp_contains(data_type, r'STRUCT')
{% endtest %}

{% test pu_cpop_selecao(model) %}
with esperado as (
with ultimos as (
    -- Selecionar a ocorrência inteira antes de projetar respostas, inclusive vazias.
    select selecionado.* from (
        select array_agg(r order by data_ocorrencia desc nulls last, data_registro desc nulls last,
            data_resposta desc nulls last, id_registro desc limit 1)[offset(0)] as selecionado
        from {{ pu_ref('int_registros_centro_pop') }} r
        where tipo_registro in ({% for assunto in pu_cpop_perfil_assuntos() %}'{{ assunto }}'{% if not loop.last %}, {% endif %}{% endfor %})
        group by id_usuario, tipo_registro
    )
), usuarios as (
    select distinct id_usuario from ultimos
)
select u.id_usuario,
    if(pai.id_registro is not null, 'Sim', '') as possui_registro_pai
    {% for assunto, campos in pu_cpop_perfil_assuntos().items() %}
        , {{ assunto }}.data_ocorrencia as data_ocorrencia_{{ assunto }},
        {{ assunto }}.origem_data_ocorrencia as origem_data_ocorrencia_{{ assunto }},
        {{ assunto }}.situacao_data_ocorrencia as situacao_data_ocorrencia_{{ assunto }},
        {{ assunto }}.data_registro as data_registro_{{ assunto }}
        , {{ assunto }}.data_resposta as data_resposta_{{ assunto }}
{% for campo in campos %}
        , {% if campo == 'encaminhamentos' %}
            array(select distinct concat(e.label, ': ', e.valor) from unnest({{ assunto }}.encaminhamentos) e
                where nullif(trim(e.label), '') is not null and nullif(trim(e.valor), '') is not null order by 1)
          {% elif campo == 'demandas' %}coalesce({{ assunto }}.{{ campo }}, [])
          {% elif pu_cpop_campos()[campo] == 'DATE' %}{{ assunto }}.{{ campo }}
          {% else %}coalesce({{ assunto }}.{{ campo }}, ''){% endif %} as {{ pu_cpop_perfil_nome(assunto, campo) }}
{% endfor %}
{% endfor %}
from usuarios u
{% for assunto in pu_cpop_perfil_assuntos() %}
left join ultimos {{ assunto }} on u.id_usuario = {{ assunto }}.id_usuario and {{ assunto }}.tipo_registro = '{{ assunto }}'
{% endfor %}
), comparacao as (
select id_usuario, to_json_string(e) valor, 1 origem from esperado e
union all
select id_usuario, to_json_string((select as struct o.* except(unidades_atendimento))) valor, -1 origem from {{ model }} o where id_usuario in (select id_usuario from esperado)
)
select id_usuario from comparacao group by id_usuario, valor having sum(origem) != 0
{% endtest %}

{% test pu_cpop_unidades(model) %}
with atendimentos as (
    select distinct p.id_usuario, nullif(trim(a.nome_unidade), '') nome
    from {{ pu_ref('int_atendimentos_por_usuario') }} p, unnest(p.atendimentos) a
    join {{ pu_ref('dim_usuarios') }} u on p.id_usuario = u.id_usuario
    where a.tipo_servico = 'centro_pop'
), esperado as (
    select id_usuario from atendimentos
    union distinct
    select id_usuario from {{ pu_ref('int_registros_centro_pop') }}
    where tipo_registro in ('pai','atendimento_social','saude','situacao_rua','desligamento')
)
 , listas as (
    select id_usuario, array_agg(distinct nome ignore nulls order by nome) unidades
    from atendimentos group by id_usuario
)
select coalesce(e.id_usuario, m.id_usuario) id_usuario
from esperado e full join {{ model }} m using(id_usuario)
left join listas l on l.id_usuario = e.id_usuario
where e.id_usuario is null or m.id_usuario is null
    or to_json_string(m.unidades_atendimento) != to_json_string(coalesce(l.unidades, []))
{% endtest %}
