{{ config(alias='dim_alta_complexidade', schema='gerenciamento__dbt', enabled=target.name != 'prod', tags=['planilha_unificada', 'pu_dimensoes']) }}
with registros as (
    select
        p.id_usuario,
        array_agg((
            select as struct
                h.*,
                coalesce(e.email_filtro, '') as email_filtro
        ) order by h.data_entrada, h.id_ciclo) as registros
    from {{ ref('int_perfil_alta_complexidade') }} as p, unnest(p.historico) as h
    left join {{ ref('int_filtro_email_unidades') }} as e on h.id_unidade = e.id_unidade
    group by p.id_usuario
)

select
    p.id_usuario,
    p.historico,
    coalesce(r.registros, []) as registros,
    array(
        select as struct ciclo.*
        from unnest(p.historico) as ciclo
        order by
            if(ciclo.data_saida is null, 0, 1), ciclo.data_entrada desc nulls last,
            ciclo.id_ciclo desc, to_json_string(ciclo) desc
        limit 1
    )[safe_offset(0)] as ultimo_ciclo
from {{ ref('int_perfil_alta_complexidade') }} as p
left join registros as r on p.id_usuario = r.id_usuario
