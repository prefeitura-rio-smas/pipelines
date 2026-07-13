-- Model responsável por conter todos os atendimentos do módulo usuário

with atendimentos_modulo_familia as (
    select
        sequs,
        seqatend,
        seqpac,
        seqfamil,
        seqprof,
        seqlogincad,
        seqtpatend,
        'f' as modulo,
        indatendcanc as flag_atendimento_cancelado,
        dsclstprof as seqprof_atendimento_compartilhado,
        indlocalatend as local_atendimento,
        dtentrada as data_atendimento,
        horaent as hora_atendimento,
        dtsaida as data_saida_atendimento,
        horasai as hora_saida_atendimento,
    from {{ source('brutos_acolherio_staging', 'gh_atend_familia') }}
)


select
    sequs,
    seqatend,
    concat(seqatend, modulo) as seqatend_modulo,
    seqpac,
    seqfamil,
    seqprof,
    seqlogincad,
    seqtpatend,
    modulo,
    flag_atendimento_cancelado,
    seqprof_atendimento_compartilhado,
    local_atendimento,
    data_atendimento,
    hora_atendimento,
    data_saida_atendimento,
    hora_saida_atendimento
from atendimentos_modulo_familia    