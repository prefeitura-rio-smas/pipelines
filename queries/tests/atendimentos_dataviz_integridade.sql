-- A agregação por usuário não pode duplicar, perder IDs ou alterar os ranks.
with atendimentos as (
    select
        a.id_atendimento_modulo,
        a.id_atendimento_sk,
        a.id_atendimento,
        a.flag_elegivel_dataviz,
        a.atendimento_unico_rank,
        a.cbo_unico_rank,
        a.tipo_atendimento
    from {{ ref('mart_atendimentos') }} as u,
        unnest(u.atendimentos) as a
)

select id_atendimento_modulo
from atendimentos
group by id_atendimento_modulo
having count(*) != 1
union all
select id_atendimento_modulo
from atendimentos
where
    id_atendimento_modulo is null
    or id_atendimento_sk is null
    or id_atendimento is distinct from id_atendimento_modulo
    or flag_elegivel_dataviz is null
    or atendimento_unico_rank is null
    or atendimento_unico_rank < 1
    or cbo_unico_rank != 1
    or tipo_atendimento is null
    or tipo_atendimento not in ('Atendimento Recepção', 'Atendimento Técnico', 'Outros Atendimentos')
