with base_atendimentos as (
    select 
        b.seqatend,
        'u' as modulo,
        b.dtentrada,
        safe_cast(trunc(safe_cast(b.horaent as float64)) as int64) as horaent,
        b.dtsaida,
        b.seqtpatend,
        b.seqprof,
        b.seqpac,
        b.sequs,
        b.datcadast,
        b.indlocalatend,
        b.indatendcanc,
        b.dsclstprof,
        b.seqlogincad,
        f.seqfamil
    from {{ source('brutos_acolherio_staging', 'gh_atendimentos') }} b
    left join {{ source('brutos_acolherio_staging', 'gh_familias_membros') }} f 
        on f.seqpac = b.seqpac
    where b.dtsaida is null

    union all

    select 
        seqatend,
        'f' as modulo,
        dtentrada,
        safe_cast(trunc(safe_cast(horaent as float64)) as int64) as horaent,
        dtsaida,
        seqtpatend,
        seqprof,
        seqpac,
        sequs,
        datcadast,
        indlocalatend,
        indatendcanc,
        dsclstprof,
        seqlogincad,
        seqfamil
    from {{ source('brutos_acolherio_staging', 'gh_atend_familia') }}
)

select * from base_atendimentos