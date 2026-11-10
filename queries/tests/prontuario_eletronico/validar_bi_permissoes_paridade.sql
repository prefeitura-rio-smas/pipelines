{{ config(tags=['prontuario_bi_unidade']) }}
with esperado as (
    select distinct
        b._id_unidade as id_unidade,
        lower(trim(email)) as email
    from {{ ref('int_atendimentos_bi') }} as b,
        unnest(split(regexp_replace(coalesce(b.email, ''), r'[;\r\n]', ','), ',')) as email
    where b._id_unidade is not null and nullif(trim(email), '') is not null
),

atual as (
    select
        a.id_unidade,
        a.email
    from {{ ref('prontuario_bi_permissoes') }} as a
    where exists (
        select 1 from esperado as e
        where e.id_unidade = a.id_unidade
    )
)

select coalesce(e.id_unidade, a.id_unidade) as id_unidade
from esperado as e full join atual as a on e.id_unidade = a.id_unidade and e.email = a.email
where e.email is null or a.email is null
