{{ config(alias='dim_creas', schema='gerenciamento__dbt', enabled=target.name != 'prod', tags=['planilha_unificada', 'pu_dimensoes']) }}
-- Preserve a ordem publicada dos campos do registro.
-- noqa: disable=ST06
with registros as (
    select
        p.id_usuario,
        array_agg((
            select as struct
                p.id_unidade,
                e.nome_unidade,
                coalesce(e.email_filtro, '') as email_filtro,
                p.creas.*
        )
        order by p.id_unidade) as registros
    from {{ ref('int_perfil_creas_por_unidade') }} as p
    left join {{ ref('int_filtro_email_unidades') }} as e on p.id_unidade = e.id_unidade
    group by p.id_usuario
)

select
    p.id_usuario,
    p.historico,
    p.participacoes,
    coalesce(r.registros, []) as registros
from {{ ref('int_perfil_creas') }} as p
left join registros as r on cast(p.id_usuario as string) = r.id_usuario
