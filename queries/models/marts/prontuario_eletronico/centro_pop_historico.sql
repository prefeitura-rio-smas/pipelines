{{ config(materialized='ephemeral', tags=['centro_pop_mensal']) }}
with fechados as (
    select * from {{ ref('centro_pop_snapshots') }}
    qualify versao = max(versao) over (partition by mes_referencia)
)

select * from fechados
union all
select p.* from {{ ref('centro_pop_mensal') }} as p
where
    p.mes_referencia = date_trunc(current_date('America/Sao_Paulo'), month)
    and not exists (
        select 1 from fechados as f
        where f.mes_referencia = p.mes_referencia
    )
