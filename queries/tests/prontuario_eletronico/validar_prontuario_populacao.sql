{{ config(enabled=target.name != 'prod' or var('prontuario_publicacao', false), tags=['planilha_unificada']) }}
-- Reconstruir os vínculos das fontes para também detectar linhas extras sem eventos.
with vinculos as (
    select
        cast(p.id_usuario as string) as id_usuario,
        a.id_unidade
    from {{ ref('int_atendimentos_por_usuario') }} as p, unnest(p.atendimentos) as a
    union distinct
    select
        cast(id_usuario as string) as id_usuario,
        id_unidade
    from {{ ref('int_evolucoes_por_usuario') }}
    union distinct
    select
        cast(id_usuario as string) as id_usuario,
        id_unidade
    from {{ ref('int_projetos_sociais') }}
    union distinct
    select
        cast(id_usuario as string) as id_usuario,
        id_unidade
    from {{ ref('int_registros_centro_pop') }}
    union distinct
    select
        cast(id_usuario as string) as id_usuario,
        id_unidade
    from {{ ref('int_centro_pop_historico_mensal') }}
    union distinct
    select
        cast(p.id_usuario as string) as id_usuario,
        h.id_unidade
    from {{ ref('int_perfil_creas') }} as p, unnest(p.perfil_geral.historico) as h
    union distinct
    select
        cast(p.id_usuario as string) as id_usuario,
        h.id_unidade
    from {{ ref('int_perfil_creas') }} as p, unnest(p.perfil_geral.participacoes) as h
    union distinct
    select
        cast(p.id_usuario as string) as id_usuario,
        h.id_unidade
    from {{ ref('alta_complexidade') }} as p, unnest(p.historico) as h
),

esperado as (
    select
        cast(u.id_usuario as string) as id_usuario,
        coalesce(cast(v.id_unidade as string), '') as id_unidade
    from {{ ref('dim_usuarios') }} as u
    left join vinculos as v on cast(u.id_usuario as string) = v.id_usuario
),

obtido as (
    select
        usuario.id_usuario,
        id_unidade
    from {{ ref('prontuario_eletronico') }}
)

select
    coalesce(e.id_usuario, o.id_usuario) as id_usuario,
    coalesce(e.id_unidade, o.id_unidade) as id_unidade
from esperado as e full join obtido as o on e.id_usuario = o.id_usuario and e.id_unidade = o.id_unidade
where e.id_usuario is null or o.id_usuario is null
