{{ config(tags=['planilha_unificada']) }}
-- A programada projeta CADASTRANTE da mesma seleção de participações.
with programada as (
    select
        coalesce(cast(_id_unidade as string), '') as id_unidade,
        nullif(trim(upper(cadastrante)), '') as cadastrante
    from {{ ref('int_acolherio_atendimentos') }}
),

prontuario as (
    select
        p.id_unidade,
        prof.id_profissional as cadastrante
    from {{ ref('prontuario_eletronico') }} as p
    cross join unnest(p.atendimentos.registros) as a
    cross join unnest(a.profissionais) as prof
),

esperado as (
    select
        id_unidade,
        grouping(id_unidade) as total,
        count(distinct cadastrante) as quantidade,
        array_agg(distinct cadastrante ignore nulls order by cadastrante) as cadastrantes
    from programada
    group by grouping sets ((), (id_unidade))
),

obtido as (
    select
        id_unidade,
        grouping(id_unidade) as total,
        count(distinct cadastrante) as quantidade,
        array_agg(distinct cadastrante ignore nulls order by cadastrante) as cadastrantes
    from prontuario
    group by grouping sets ((), (id_unidade))
)

select
    e.quantidade as quantidade_programada,
    o.quantidade as quantidade_prontuario,
    coalesce(e.total, o.total) as total,
    coalesce(e.id_unidade, o.id_unidade) as id_unidade
from esperado as e
full outer join obtido as o on e.total = o.total and e.id_unidade is not distinct from o.id_unidade
where
    e.quantidade is distinct from o.quantidade
    or to_json_string(e.cadastrantes) is distinct from to_json_string(o.cadastrantes)
