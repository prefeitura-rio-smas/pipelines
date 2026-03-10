-- Teste para verificar se a flag do atendimento compartilhado está correta.

with teste_flag_compartilhado as (
    select
        *
    from {{ ref('int_atendimentos') }}
    where seqprof_atendimento_compartilhado is null or trim(seqprof_atendimento_compartilhado) = ''
    and flag_atendimento_compartilhado = 'Sim'
)

select * from teste_flag_compartilhado