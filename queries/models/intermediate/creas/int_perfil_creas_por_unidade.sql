{{ config(materialized='ephemeral', enabled=target.name != 'prod', tags=['planilha_unificada']) }}
with perfil as (
    with base_historico as (
        select
            concat(cast(p.id_usuario as string), ':', cast(h.id_unidade as string)) as id_usuario,
            array_agg(h) as historico
        from {{ ref('creas') }} as p, unnest(p.historico) as h
        where h.id_unidade is not null
        group by 1
    ),

    base_participacoes as (
        select
            concat(cast(p.id_usuario as string), ':', cast(h.id_unidade as string)) as id_usuario,
            array_agg(h order by h.data_presenca, h.hora_presenca, h.id_atividade) as participacoes
        from {{ ref('creas') }} as p, unnest(p.participacoes) as h
        where h.id_unidade is not null
        group by 1
    ),

    creas_escopo as (
        select
            coalesce(h.id_usuario, p.id_usuario) as id_usuario,
            coalesce(h.historico, []) as historico,
            coalesce(p.participacoes, []) as participacoes
        from base_historico as h full join base_participacoes as p on h.id_usuario = p.id_usuario
    ),

    perfil_scoped as ({{ pu_perfil_creas_sql('creas_escopo') }})

    select * from perfil_scoped
)

select
    split(id_usuario, ':')[offset(0)] as id_usuario,
    safe_cast(split(id_usuario, ':')[offset(1)] as int64) as id_unidade,
    (select as struct perfil.* except (id_usuario)) as creas
from perfil
