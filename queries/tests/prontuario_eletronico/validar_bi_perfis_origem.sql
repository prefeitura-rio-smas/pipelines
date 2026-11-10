{{ config(tags=['prontuario_bi_unidade']) }}
with pai as (
    select
        id_usuario,
        id_unidade,
        array_agg(
            r order by
                data_ocorrencia desc nulls last, data_registro desc nulls last,
                data_resposta desc nulls last, id_registro desc
            limit 1
        )[
            offset(0
            )
        ] as ultimo
    from {{ pu_ref('int_registros_centro_pop') }} as r
    where tipo_registro = 'pai' and id_unidade is not null
    group by id_usuario, id_unidade
),

encaminhamentos as (
    select
        r.id_usuario,
        r.id_unidade,
        string_agg(distinct concat(trim(e.label), ': ', trim(e.valor)), ', ' order by concat(trim(e.label), ': ', trim(e.valor))) as esperado
    from {{ pu_ref('int_registros_centro_pop') }} as r, unnest(r.encaminhamentos) as e
    where
        r.tipo_registro = 'pai' and r.id_unidade is not null
        and nullif(trim(e.label), '') is not null and nullif(trim(e.valor), '') is not null
    group by r.id_usuario, r.id_unidade
)

select p.id_usuario
from {{ ref('prontuario_bi_centro_pop') }} as p
left join pai as a on p.id_usuario = cast(a.id_usuario as string) and p.id_unidade = a.id_unidade
left join encaminhamentos as e on p.id_usuario = cast(e.id_usuario as string) and p.id_unidade = e.id_unidade
where
    p.centro_pop.data_registro_pai is distinct from coalesce(cast(a.ultimo.data_registro as string), '')
    or p.centro_pop.encaminhamentos_pai is distinct from coalesce(e.esperado, '')
union all
select id_usuario
from {{ ref('prontuario_bi_creas') }}
where
    (creas.mse_id_unidade is not null and creas.mse_id_unidade != id_unidade)
    or (creas.mse_retorno_id_unidade is not null and creas.mse_retorno_id_unidade != id_unidade)
    or (creas.peti_id_unidade is not null and creas.peti_id_unidade != id_unidade)
    or (creas.peti_desligamento_id_unidade is not null and creas.peti_desligamento_id_unidade != id_unidade)
    or (creas.scfv_id_unidade is not null and creas.scfv_id_unidade != id_unidade)
