-- Falha se o mart perder atendimentos, multiplicar linhas ou perder profissionais.
with origem as (
    select
        id_atendimento_modulo,
        count(*) as quantidade_profissionais
    from {{ ref('int_atendimentos_completos') }}
    group by id_atendimento_modulo
),

mart as (
    select
        id_atendimento_modulo,
        count(*) as quantidade_linhas,
        sum(array_length(profissionais)) as quantidade_profissionais
    from {{ ref('mart_atendimentos') }}
    group by id_atendimento_modulo
)

select
    o.quantidade_profissionais as quantidade_origem,
    m.quantidade_profissionais as quantidade_mart,
    m.quantidade_linhas,
    coalesce(o.id_atendimento_modulo, m.id_atendimento_modulo) as id_atendimento_modulo
from origem as o
full outer join mart as m on o.id_atendimento_modulo = m.id_atendimento_modulo
where
    m.quantidade_linhas is distinct from 1
    or o.quantidade_profissionais is distinct from m.quantidade_profissionais
