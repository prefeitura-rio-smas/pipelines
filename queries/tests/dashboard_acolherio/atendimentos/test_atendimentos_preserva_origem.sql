-- Falha se o enriquecimento perder ou multiplicar um atendimento × profissional.
with origem as (
    select
        {{ dbt_utils.generate_surrogate_key(['id_atendimento_modulo', 'id_profissional']) }} as id_atendimento_sk,
        count(*) as quantidade
    from {{ ref('int_atendimentos_completos') }}
    group by id_atendimento_sk
),

mart as (
    select
        id_atendimento_sk,
        count(*) as quantidade
    from {{ ref('mart_atendimentos') }}
    group by id_atendimento_sk
)

select
    o.quantidade as quantidade_origem,
    m.quantidade as quantidade_mart,
    coalesce(o.id_atendimento_sk, m.id_atendimento_sk) as id_atendimento_sk
from origem as o
full outer join mart as m on o.id_atendimento_sk = m.id_atendimento_sk
where o.quantidade is distinct from m.quantidade
