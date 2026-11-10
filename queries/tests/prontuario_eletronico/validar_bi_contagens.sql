{{ config(tags=['prontuario_bi_unidade']) }}
with esperado as (
    select
        _id_unidade as id_unidade,
        count(*) as quantidade
    from {{ ref('int_atendimentos_bi') }}
    where _id_unidade is not null
    group by _id_unidade
),

linhas as (
    select
        id_unidade,
        count(*) as quantidade
    from {{ ref('prontuario_bi_linhas_profissionais') }}
    group by id_unidade
),

eventos as (
    select
        id_unidade,
        sum(quantidade_linhas_bi) as quantidade
    from {{ ref('prontuario_bi_atendimentos') }}
    group by id_unidade
)

select coalesce(e.id_unidade, l.id_unidade, v.id_unidade) as id_unidade
from esperado as e full join linhas as l on e.id_unidade = l.id_unidade
full join eventos as v on coalesce(e.id_unidade, l.id_unidade) = v.id_unidade
where e.quantidade is distinct from l.quantidade or e.quantidade is distinct from v.quantidade
