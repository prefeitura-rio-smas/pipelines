{{ config(tags=['planilha_unificada']) }}
with esperado as (
    select
        cast(s.id_usuario as string) as id_usuario,
        'historico' as bloco,
        to_json_string(struct({{ pu_cpop_historico_publico('s') }})) as registro
    from {{ ref('int_pu_registros_centro_pop') }} as s
    where s.tipo_registro != 'participacao'
    union all
    select
        cast(s.id_usuario as string) as id_usuario,
        'participacoes' as bloco,
        to_json_string(struct(
            coalesce(cast(s.id_atividade as string), '') as id_atividade,
            coalesce(s.nome_atividade, '') as nome_atividade,
            coalesce(s.nome_unidade, '') as nome_unidade,
            coalesce(cast(s.data_presenca as string), '') as data_presenca,
            coalesce(s.hora_presenca, '') as hora_presenca
        )) as registro
    from {{ ref('int_pu_registros_centro_pop') }} as s
    where s.tipo_registro = 'participacao'
),

obtido as (
    select
        p.usuario.id_usuario,
        'historico' as bloco,
        to_json_string(h) as registro
    from {{ ref('planilha_unificada') }} as p, unnest(p.escopos.centro_pop.historico) as h
    union all
    select
        p.usuario.id_usuario,
        'participacoes' as bloco,
        to_json_string(h) as registro
    from {{ ref('planilha_unificada') }} as p, unnest(p.escopos.centro_pop.participacoes) as h
),

contagens as (
    select
        id_usuario,
        bloco,
        registro,
        count(*) as quantidade
    from esperado
    group by 1, 2, 3
),

contagens_obtidas as (
    select
        id_usuario,
        bloco,
        registro,
        count(*) as quantidade
    from obtido
    group by 1, 2, 3
)

select coalesce(e.id_usuario, o.id_usuario) as id_usuario
from contagens as e full outer join contagens_obtidas as o on e.id_usuario = o.id_usuario and e.bloco = o.bloco and e.registro = o.registro
where e.quantidade is distinct from o.quantidade
union all
select p.usuario.id_usuario from {{ ref('planilha_unificada') }} as p
where
    p.escopos is null or p.escopos.centro_pop is null
    or exists (
        select 1 from unnest(p.escopos.centro_pop.historico) as h
        where
            h.origem is null or h.tipo_registro is null or h.abrangencia is null
            or h.id_evolucao_modulo is null or h.titulo_formulario is null or h.nome_unidade is null
            or h.data_registro is null or h.data_resposta is null
            {% for campo, tipo in pu_cpop_campos().items() if campo not in ['id_atividade', 'nome_atividade', 'data_presenca', 'hora_presenca'] and not tipo.startswith('ARRAY') %}
                or h.{{ campo }} is null
            {% endfor %}
            or exists (
                select 1 from unnest(h.demandas) as demanda
                where demanda is null
            )
            or exists (
                select 1 from unnest(h.encaminhamentos) as e
                where e.label is null or e.valor is null
            )
    )
    or exists (
        select 1 from unnest(p.escopos.centro_pop.participacoes) as h
        where h.id_atividade is null or h.nome_atividade is null or h.nome_unidade is null or h.data_presenca is null or h.hora_presenca is null
    )
