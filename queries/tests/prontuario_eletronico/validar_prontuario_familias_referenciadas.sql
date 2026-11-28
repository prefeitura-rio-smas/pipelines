{{ config(tags=['planilha_unificada'], enabled=target.name in ['dev', 'ci'] and var('validar_familias_com_programada', false)) }}
with esperado as (
    select distinct
        cast(id_usuario as string) as id_usuario,
        upper(trim(coalesce(unidade_atendimento, ''))) as unidade,
        nullif(trim(cast(id_familia as string)), '') as id_familia
    from {{ source('programada_prontuario', 'dev_atendimentos') }}
    where nullif(trim(cast(id_familia as string)), '') is not null
),

obtido as (
    select distinct
        p.id_usuario,
        upper(trim(coalesce(p.nome_unidade, ''))) as unidade,
        nullif(trim(f.id_familia), '') as id_familia
    from {{ ref('prontuario_eletronico') }} as p
    cross join unnest(p.familias) as f
    where nullif(trim(f.id_familia), '') is not null
)

select
    coalesce(e.id_usuario, o.id_usuario) as id_usuario,
    coalesce(e.unidade, o.unidade) as unidade,
    coalesce(e.id_familia, o.id_familia) as id_familia,
    if(o.id_familia is null, 'referencia_sem_familia', 'familia_sem_referencia') as divergencia
from esperado as e
full join obtido as o
    on
        e.id_usuario = o.id_usuario
        and e.unidade = o.unidade
        and e.id_familia = o.id_familia
where e.id_familia is null or o.id_familia is null
