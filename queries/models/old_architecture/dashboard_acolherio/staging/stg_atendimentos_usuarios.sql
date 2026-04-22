-- Model responsável por conter todos os atendimentos do módulo usuário

with atendimentos_modulo_usuario as (
    select
        sequs,
        seqatend,
        seqpac,
        seqprof,
        seqlogincad,
        seqtpatend,
        seqativgrp,
        codclin,
        'u' as modulo,
        indatendcanc as flag_atendimento_cancelado,
        dsclstprof as seqprof_atendimento_compartilhado,
        indlocalatend as local_atendimento,
        dtentrada as data_atendimento,
        horaent as hora_atendimento,
        dtsaida as data_saida_atendimento,
        horasai as hora_saida_atendimento,
    from {{ source('brutos_acolherio_staging', 'gh_atendimentos') }}
)

select
    sequs,
    concat(seqatend, modulo) as seqatend_modulo,
    seqatend,
    seqpac,
    seqprof,
    seqlogincad,
    seqtpatend,
    seqativgrp,
    codclin,
    modulo,
    flag_atendimento_cancelado,
    seqprof_atendimento_compartilhado,
    local_atendimento,
    data_atendimento,
    hora_atendimento,
    data_saida_atendimento,
    hora_saida_atendimento
from atendimentos_modulo_usuario