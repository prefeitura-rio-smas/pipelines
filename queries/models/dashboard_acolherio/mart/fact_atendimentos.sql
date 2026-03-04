-- Tabela fato de atendimentos.
-- Grão 1 atendimento.

with fato_atendimentos as (
    select
        atend.seqatend_modulo,
        user.seqpac_sk,
        prof.seqprof_sk,
        unid.sequs_sk,
        tp_atend.seqtpatend_sk
    from {{ ref('int_atendimentos') }} atend 
    left join {{ ref('dim_usuarios') }} user on atend.seqpac = user.seqpac
    left join {{ ref('dim_profissionais') }} prof on atend.seqprof = prof.seqprof
    left join {{ ref('dim_unidades') }} unid on unid.sequs = atend.sequs
    left join {{ ref('dim_tipo_atendimento') }} tp_atend on atend.seqtpatend = tp_atend.seqtpatend
)

select * from fato_atendimentos