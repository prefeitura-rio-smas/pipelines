-- Tabela dimensão responsavel por todos os atendimentos compartilhados.

-- Normalização do seqprof.
with normalizar_seqprof as (
select
    concat(seqprof, ',', seqprof_atendimento_compartilhado) as total_prof_atendimento,
    seqatend_modulo,
    seqatend,
    seqtpatend,
    sequs,
    seqpac,
    data_atendimento,
    hora_atendimento
from {{ ref('int_atendimentos_no_row_number') }}
where flag_atendimento_compartilhado = 'Sim'
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
    cast(explodir_seqprof as int64) as seqprof_compartilhado_tratado,
from normalizar_seqprof,
unnest(split(total_prof_atendimento, ',')) as explodir_seqprof
),

retirar_duplicada as (
select
    *,
    row_number() over(
        partition by
            seqprof_compartilhado_tratado,
            data_atendimento,
            hora_atendimento,
            seqtpatend,
            seqpac,
            sequs
        order by seqatend asc
    ) as rn_v2
from explodir_profissional
),



-- Captura todos os atendimentos que são compartilhados.
atendimentos_compartilhados as (
    select
        a.seqprof_compartilhado_tratado,
        a.seqatend_modulo,
        a.seqatend,
        user.seqpac_sk,
        prof.seqprof_sk,
        unid.sequs_sk,
        tipo_atend.seqtpatend_sk
    from retirar_duplicada a 
    left join {{ ref('dim_tipo_atendimento') }} tipo_atend on tipo_atend.seqtpatend = a.seqtpatend
    left join {{ ref('dim_profissionais') }} prof on prof.seqprof = a.seqprof_compartilhado_tratado
    left join {{ ref('dim_unidades') }} unid on unid.sequs = a.sequs
    left join {{ ref('dim_usuarios') }} user on user.seqpac = a.seqpac
    where a.rn_v2 = 1
)

select * from atendimentos_compartilhados