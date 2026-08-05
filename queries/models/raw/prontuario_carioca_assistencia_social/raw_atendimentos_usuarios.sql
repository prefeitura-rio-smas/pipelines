-- Camada Raw: Atendimentos do Módulo Usuário
with source as (
    select
        sequs as id_unidade,
        seqatend as id_atendimento,
        seqpac as id_paciente,
        seqprof as id_profissional,
        seqlogincad as id_login_cadastro,
        seqtpatend as id_tipo_atendimento,
        seqativgrp as id_atividade_grupo,
        codclin as codigo_clinica,
        'u' as modulo,
        indatendcanc as flag_cancelado,
        dsclstprof as id_profissional_compartilhado,
        indlocalatend as local_atendimento,
        dtentrada as data_atendimento,
        horaent as hora_atendimento,
        dtsaida as data_saida,
        horasai as hora_saida,
    from {{ source('brutos_acolherio_staging', 'gh_atendimentos') }}
)
select 
    *,
    concat(id_atendimento, modulo) as id_atendimento_modulo
from source
