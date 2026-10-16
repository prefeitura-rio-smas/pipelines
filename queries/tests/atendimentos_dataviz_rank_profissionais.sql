-- Cada grupo elegível tem exatamente um representante, inclusive os
-- compartilhados. Registros inelegíveis não podem ocupar seu primeiro rank.
select
    a.hora_de_atendimento,
    a.data_de_atendimento,
    a.nome_atendimento_original,
    u.id_usuario,
    a.unidade_atendimento,
    if(p.flag_cadastrado, p.id_profissional, null) as id_profissional
from {{ ref('mart_atendimentos') }} as u,
    unnest(u.atendimentos) as a,
    unnest(a.profissionais) as p
where a.flag_elegivel_dataviz
group by 1, 2, 3, 4, 5, 6
having countif(p.atendimento_unico_rank = 1) != 1
