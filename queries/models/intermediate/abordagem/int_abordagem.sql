-- Camada Intermediate: entidade de abordagem (conformada).
-- Consolida o formulário repeat, o cluster de dedup, a ficha de origem e o
-- de/para de e-mails. A mart_abordagem é apenas a apresentação final.
-- Grão: 1 linha por abordagem (globalid).
-- O ficha_objectid é nulo em toda a base (join legado pendente de investigação),
-- mantido para preservar o contrato de 154 colunas.
{{ config(materialized = 'ephemeral') }}

with repeat as (
    select * from {{ ref('raw_abordagem_repeat') }}
),

dedup as (
    select * from {{ ref('int_abordagem_repeat_dedup') }}
),

emails as (
    select * from {{ ref('raw_abordagem_filtro_emails') }}
),

ficha as (
    select objectid from {{ ref('raw_abordagem_ficha') }}
)

select
    r.*,  -- noqa: AM04
    e.cas,
    e.e_mail,
    c.cluster_id,
    c.is_duplicado,
    f.objectid as ficha_objectid
from repeat as r
left join emails as e on r.repeat_unidade_cas = e.cas
left join dedup as c on r.globalid = c.globalid
left join ficha as f on r.parentrowid = cast(f.objectid as string)
