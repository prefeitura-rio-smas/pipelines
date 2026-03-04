-- Tabela dimensão responsavel por todos os atendimentos compartilhados.

-- Captura todos os atendimentos que são compartilhados.
with atendimentos_compartilhados as (
    select
        seqatend_modulo,
        seqprof_atendimento_compartilhado
    from `rj-smas-dev.relatorio.int_atendimentos` atend 
    where seqprof_atendimento_compartilhado != ''
),

-- Explode os profissionais
explodir_profissional as (
select
    seqatend_modulo,
    cast(seqprof_compartilhado as int64) as seqprof_compartilhado_tratado,
from atendimentos_compartilhados,
unnest(split(seqprof_atendimento_compartilhado, ',')) as seqprof_compartilhado
)

select
    a.seqprof_compartilhado_tratado,
    a.seqatend_modulo,
    b.seqprof_sk
from explodir_profissional a 
left join `rj-smas-dev.relatorio.dim_profissionais`b on a.seqprof_compartilhado_tratado = b.seqprof