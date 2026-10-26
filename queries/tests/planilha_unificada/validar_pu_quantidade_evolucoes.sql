{{ config(tags = ['planilha_unificada']) }}

-- Confere a quantidade com a origem, sem depender da extração dos formulários.
with esperado as (
    select
        e.id_usuario,
        count(distinct e.id_evolucao_modulo) as quantidade
    from {{ pu_ref('int_pu_evolucoes') }} as e
    where e.data_cancelamento is null
    group by 1
)

select p.usuario.id_usuario
from {{ ref('planilha_unificada') }} as p
left join esperado as e on p.usuario.id_usuario = cast(e.id_usuario as string)
where
    p.evolucoes.quantidade is distinct from coalesce(e.quantidade, 0)
    or p.evolucoes.quantidade != array_length(p.evolucoes.registros)
    or p.evolucoes.quantidade != (
        select count(distinct v.id_evolucao_modulo) from unnest(p.evolucoes.registros) as v
    )
    or exists (
        select 1 from unnest(p.evolucoes.registros) as v
        where nullif(trim(v.data_cancelamento), '') is not null
    )
