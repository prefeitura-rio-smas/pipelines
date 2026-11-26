{{ config(materialized='ephemeral', enabled=target.name != 'prod' or var('prontuario_publicacao', false), tags=['planilha_unificada']) }}
with perfil_geral as (
    {{ perfil_centro_pop(ref('int_registros_centro_pop'), ref('int_atendimentos_por_usuario'), ref('dim_usuarios')) }}
),

registros_escopo as (
    select r.* replace (concat(cast(r.id_usuario as string), ':', coalesce(cast(r.id_unidade as string), '')) as id_usuario)
    from {{ ref('int_registros_centro_pop') }} as r
),

atendimentos_escopo as (
    select
        concat(cast(p.id_usuario as string), ':', coalesce(cast(a.id_unidade as string), '')) as id_usuario,
        array_agg(a) as atendimentos
    from {{ ref('int_atendimentos_por_usuario') }} as p, unnest(p.atendimentos) as a
    group by id_usuario
),

usuarios_escopo as (
    select id_usuario from registros_escopo
    union distinct
    select id_usuario from atendimentos_escopo
),

perfil_scoped as ({{ perfil_centro_pop('registros_escopo', 'atendimentos_escopo', 'usuarios_escopo') }}),

participacoes as (
    select * from registros_escopo
    where tipo_registro = 'participacao'
    qualify row_number() over (
        partition by
            id_usuario order by
            data_presenca desc nulls last,
            hora_presenca desc nulls last, id_atividade desc nulls last, id_registro desc
    ) = 1
),

perfil_por_unidade as (
    select
        split(coalesce(perfil.id_usuario, part.id_usuario), ':')[offset(0)] as id_usuario,
        safe_cast(split(coalesce(perfil.id_usuario, part.id_usuario), ':')[offset(1)] as int64) as id_unidade,
        (select as struct perfil.* except (id_usuario)) as perfil,
        struct(
            {{ perfil_publico_centro_pop('perfil', listas_em_texto=true) }},
            part.nome_atividade as ultima_participacao_atividade,
            part.nome_unidade as ultima_participacao_unidade,
            part.data_presenca as ultima_participacao_data,
            part.hora_presenca as ultima_participacao_hora
        ) as centro_pop
    from perfil_scoped as perfil full join participacoes as part on perfil.id_usuario = part.id_usuario
),

registros_por_usuario as (
    select
        u.id_usuario,
        array_agg((select as struct u.* except (id_usuario)) order by u.id_unidade) as registros
    from perfil_por_unidade as u
    group by u.id_usuario
)

select
    coalesce(cast(g.id_usuario as string), u.id_usuario) as id_usuario,
    if(g.id_usuario is null, null, g) as perfil_geral,
    coalesce(u.registros, []) as registros
from perfil_geral as g
full join registros_por_usuario as u on cast(g.id_usuario as string) = u.id_usuario
