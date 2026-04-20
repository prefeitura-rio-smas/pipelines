{{ config(store_failures = true) }}

with base as (
    select
        seqatend_modulo,
        seqatend,
        seqtpatend,
        sequs,
        seqpac,
        data_atendimento,
        hora_atendimento,
        concat(seqprof, ',', seqprof_atendimento_compartilhado) as total_prof_atendimento
    from {{ ref('int_atendimentos') }}
    where flag_atendimento_compartilhado = 'Sim'
      and rn_v2 = 1
),

explodir_profissional as (
    select
        seqatend_modulo,
        seqatend,
        seqtpatend,
        sequs,
        seqpac,
        data_atendimento,
        hora_atendimento,
        cast(trim(prof) as int64) as seqprof_compartilhado_tratado
    from base,
    unnest(split(total_prof_atendimento, ',')) as prof
),


total_int as (
    select
        count(*) as total_compartilhados_int
    from explodir_profissional
),

total_mart as (
    select
        count(*) as total_compartilhados_mart
    from {{ ref('dim_atendimento_compartilhado') }}
)

select *
from total_int
cross join total_mart
where total_compartilhados_int != total_compartilhados_mart