-- Tabela dimensão responsavel por todos os atendimentos compartilhados.

-- Captura todos os atendimentos que são compartilhados.
with atendimentos_compartilhados as (
    select
        sequs,
        seqatend,
        seqatend_modulo,
        concat(seqprof, ',', seqprof_atendimento_compartilhado) as total_prof_atendimento,
        seqprof,
        seqprof_atendimento_compartilhado
    from {{ ref('int_atendimentos') }} atend 
    where flag_atendimento_compartilhado = 'Sim'
),

-- Explode os profissionais
explodir_profissional as (
select
    seqatend,
    sequs,
    seqatend_modulo,
    seqprof,
    total_prof_atendimento,
    cast(teste as int64) as seqprof_compartilhado_tratado,
from atendimentos_compartilhados,
unnest(split(total_prof_atendimento, ',')) as teste
)

select
    a.seqatend,
    a.sequs,
    a.seqprof_compartilhado_tratado,
    a.total_prof_atendimento,
    a.seqatend_modulo,
    b.seqprof_sk
from explodir_profissional a 
left join {{ ref('dim_profissionais') }} b on a.seqprof_compartilhado_tratado = b.seqprof