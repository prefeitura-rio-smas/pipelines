{{ config(alias='atendimentos', tags=['acolherio_atendimentos_refresh']) }}

-- Uma linha por atendimento. A seleção analítica acontece no dataviz.
select
    a.*,
    -- O CBO principal já é único; os demais ficam nos detalhes da equipe.
    1 as cbo_unico_rank,
    (
        select p.atendimento_unico_rank
        from unnest(a.profissionais) as p
        where p.flag_principal
    ) as atendimento_unico_rank
from {{ ref('int_atendimentos_dataviz') }} as a
