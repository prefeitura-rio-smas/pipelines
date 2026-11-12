{{ config(materialized='ephemeral', enabled=target.name != 'prod', tags=['planilha_unificada']) }}
with registros_escopo as (
    select r.* replace (concat(cast(r.id_usuario as string), ':', cast(r.id_unidade as string)) as id_usuario)
    from {{ pu_ref('int_registros_centro_pop') }} as r
    where r.id_unidade is not null
),

atendimentos_escopo as (
    select
        concat(cast(p.id_usuario as string), ':', cast(a.id_unidade as string)) as id_usuario,
        array_agg(a) as atendimentos
    from {{ ref('int_atendimentos_por_usuario') }} as p, unnest(p.atendimentos) as a
    where a.id_unidade is not null
    group by 1
),

usuarios_escopo as (
    select id_usuario from registros_escopo
    union distinct
    select id_usuario from atendimentos_escopo
),

perfil_scoped as ({{ pu_perfil_centro_pop_sql('registros_escopo', 'atendimentos_escopo', 'usuarios_escopo') }}),

participacoes as (
    select * from registros_escopo
    where tipo_registro = 'participacao'
    qualify row_number() over (
        partition by
            id_usuario order by
            data_presenca desc nulls last,
            hora_presenca desc nulls last, id_atividade desc nulls last, id_registro desc
    ) = 1
)

select
    split(coalesce(perfil.id_usuario, part.id_usuario), ':')[offset(0)] as id_usuario,
    safe_cast(split(coalesce(perfil.id_usuario, part.id_usuario), ':')[offset(1)] as int64) as id_unidade,
    struct(
        {{ pu_cpop_perfil_publico('perfil', listas_em_texto=true) }},
        part.nome_atividade as ultima_participacao_atividade,
        part.nome_unidade as ultima_participacao_unidade,
        part.data_presenca as ultima_participacao_data,
        part.hora_presenca as ultima_participacao_hora
    ) as centro_pop
from perfil_scoped as perfil full join participacoes as part on perfil.id_usuario = part.id_usuario
