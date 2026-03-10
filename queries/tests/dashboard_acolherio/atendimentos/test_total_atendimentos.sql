-- Teste para verificar se o total de atendimentos da raw_atendimentos está de acordo com o total de atendimentos da mart_atendimentos

with total_atendimentos_usuarios_stg as (
    select 
        count(seqatend) as total_atend_usuario
    from {{ ref('stg_atendimentos_usuarios') }}
),

total_atendimentos_familia_stg as (
    select 
        count(seqatend) as total_atend_familia
    from {{ ref('stg_atendimentos_familias') }}
),

total_atendimentos_staging as (
    select
        ( u.total_atend_usuario + f.total_atend_familia ) as total_atendimentos_stg
    from total_atendimentos_usuarios_stg u 
    cross join total_atendimentos_familia_stg f
),

total_atendimentos_intermediate as (
    select 
        count(seqatend_modulo) as total_atendimento_model_int
    from {{ ref('int_atendimentos') }}
)

select * 
from total_atendimentos_staging stg
cross join total_atendimentos_intermediate intt
where stg.total_atendimentos_stg != intt.total_atendimento_model_int