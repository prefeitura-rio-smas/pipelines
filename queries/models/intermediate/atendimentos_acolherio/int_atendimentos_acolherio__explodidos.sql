with base as (
    select * from {{ ref('stg_atendimentos_acolherio__atendimentos') }}
),

atendimentos_explodidos as (
    -- Linhas originais
    select 
        seqatend,
        modulo,
        safe_cast(trim(cast(seqprof as string)) as int64) as seqprof,
        dtentrada,
        horaent,
        dtsaida,
        seqtpatend,
        seqpac,
        seqfamil,
        sequs,
        datcadast,
        indlocalatend,
        indatendcanc,
        dsclstprof,
        seqlogincad
    from base

    union all

    -- Linhas explodidas da lista de profissionais
    select 
        seqatend,
        modulo,
        safe_cast(trim(regexp_replace(prof_id, r'^0+', '')) as int64) as seqprof,
        dtentrada,
        horaent,
        dtsaida,
        seqtpatend,
        seqpac,
        seqfamil,
        sequs,
        datcadast,
        indlocalatend,
        indatendcanc,
        dsclstprof,
        seqlogincad
    from base,
    unnest(split(dsclstprof)) as prof_id
    where prof_id is not null 
      and trim(prof_id) != ''
)

select * from atendimentos_explodidos