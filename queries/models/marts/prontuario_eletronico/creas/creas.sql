{{ config(alias='dim_creas', schema='gerenciamento__dbt', tags=['pu_dimensoes']) }}
-- Preserve a ordem publicada dos campos do registro.
-- noqa: disable=ST06
with registros as (
    select
        p.id_usuario,
        array_agg((
            select as struct
                u.id_unidade,
                e.nome_unidade,
                coalesce(e.email_filtro, '') as email_filtro,
                u.creas.*
        )
        order by u.id_unidade) as registros
    from {{ ref('int_perfil_creas') }} as p
    cross join unnest(p.registros) as u
    left join {{ ref('int_filtro_email_unidades') }} as e on u.id_unidade = e.id_unidade
    group by p.id_usuario
)

select
    p.perfil_geral.id_usuario,
    p.perfil_geral.historico,
    p.perfil_geral.participacoes,
    coalesce(r.registros, []) as registros
from {{ ref('int_perfil_creas') }} as p
left join registros as r on p.id_usuario = r.id_usuario
where p.perfil_geral is not null
