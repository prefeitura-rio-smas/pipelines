-- A mart deve preservar o universo de atendimentos das duas fontes,
-- inclusive testes, vínculos encerrados e repetições analíticas.
with esperados as (
    select id_atendimento_modulo
    from {{ ref('raw_atendimentos_familias') }}
    union distinct
    select id_atendimento_modulo
    from {{ ref('raw_atendimentos_usuarios') }}
),

publicados as (
    select a.id_atendimento_modulo
    from {{ ref('mart_atendimentos') }} as u,
        unnest(u.atendimentos) as a
),

ausentes as (
    select id_atendimento_modulo from esperados
    except distinct
    select id_atendimento_modulo from publicados
),

inesperados as (
    select id_atendimento_modulo from publicados
    except distinct
    select id_atendimento_modulo from esperados
)

select id_atendimento_modulo from ausentes
union all
select id_atendimento_modulo from inesperados
