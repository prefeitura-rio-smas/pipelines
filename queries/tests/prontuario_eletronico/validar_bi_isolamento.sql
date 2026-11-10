-- UNION branches inherit the column names from the first projection.
-- noqa: disable=AL03
{{ config(tags=['prontuario_bi_unidade']) }}
with eventos as (
    select
        'pessoa' as id_usuario,
        1 as id_unidade,
        3 as quantidade
    union all
    select
        'pessoa',
        2,
        2
    union all
    select
        'pessoa',
        cast(null as int64),
        99
),

acl as (
    select
        'a@teste.invalid' as email,
        1 as id_unidade
    union all
    select
        'a@teste.invalid',
        1
    union all
    select
        'b@teste.invalid',
        2
    union all
    select
        'gestao@teste.invalid',
        1
    union all
    select
        'gestao@teste.invalid',
        2
),

cenarios as (
    select
        'a@teste.invalid' as email,
        3 as esperado
    union all
    select
        'b@teste.invalid',
        2
    union all
    select
        'gestao@teste.invalid',
        5
    union all
    select
        'desconhecido@teste.invalid',
        0
    union all
    select
        '',
        0
    union all
    select
        cast(null as string),
        0
)

select c.email
from cenarios as c
where c.esperado != coalesce((
    select sum(e.quantidade) from eventos as e
    where exists (
        select 1 from acl as a
        where a.id_unidade = e.id_unidade and a.email = lower(trim(c.email))
    )
), 0)
