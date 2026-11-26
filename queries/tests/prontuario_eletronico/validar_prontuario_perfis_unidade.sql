{{ config(enabled=target.name != 'prod' or var('prontuario_publicacao', false), tags=['planilha_unificada']) }}
-- Comparar valores por campo: a ordem das chaves JSON não faz parte da regra do perfil.
-- Indicadores médicos vêm do cadastro; a participação tem apresentação própria na mart.
with perfis_centro_pop as (
    select
        p.id_usuario,
        u.id_unidade,
        u.centro_pop
    from {{ ref('int_perfil_centro_pop') }} as p
    cross join unnest(p.registros) as u
),

perfis_creas as (
    select
        p.id_usuario,
        u.id_unidade,
        u.creas
    from {{ ref('int_perfil_creas') }} as p
    cross join unnest(p.registros) as u
)

select
    p.usuario.id_usuario,
    p.id_unidade,
    'centro_pop' as assunto
from {{ ref('prontuario_eletronico') }} as p
inner join perfis_centro_pop as perfil
    on
        p.usuario.id_usuario = perfil.id_usuario
        and p.id_unidade = coalesce(cast(perfil.id_unidade as string), '')
where
    exists (
        select 1 from unnest(json_keys(to_json(perfil.centro_pop))) as campo
        where
            campo not in (
                'uso_substancias_psicoativas', 'possui_problema_saude', 'ultima_participacao_atividade',
                'ultima_participacao_unidade', 'ultima_participacao_data', 'ultima_participacao_hora'
            )
            and to_json_string(to_json(p.centro_pop)[campo]) is distinct from to_json_string(to_json(perfil.centro_pop)[campo])
    )
union all
select
    p.usuario.id_usuario,
    p.id_unidade,
    'creas' as assunto
from {{ ref('prontuario_eletronico') }} as p
inner join perfis_creas as perfil
    on
        p.usuario.id_usuario = perfil.id_usuario
        and p.id_unidade = coalesce(cast(perfil.id_unidade as string), '')
where exists (
    select 1 from unnest(json_keys(to_json(perfil.creas))) as campo
    where to_json_string(to_json(p.creas)[campo]) is distinct from to_json_string(to_json(perfil.creas)[campo])
)
