{{ config(alias='dim_alta_complexidade', schema='gerenciamento__dbt', enabled=target.name != 'prod', tags=['planilha_unificada', 'pu_dimensoes']) }}
with historico as (
    select
        a.id_usuario,
        array_agg(struct(
            a.id_ciclo, a.id_unidade, u.nome_unidade,
            a.data_entrada, a.data_saida, a.motivo_saida,
            date_diff(coalesce(a.data_saida, current_date('America/Sao_Paulo')), a.data_entrada, day)
                as tempo_permanencia_dias
        ) order by a.data_entrada, a.id_ciclo) as historico
    from {{ ref('raw_usuarios_acolhimentos') }} as a
    left join {{ ref('dim_unidades') }} as u on a.id_unidade = u.id_unidade
    group by a.id_usuario
),

registros as (
    select
        p.id_usuario,
        array_agg((
            select as struct
                h.*,
                coalesce(e.email_filtro, '') as email_filtro
        ) order by h.data_entrada, h.id_ciclo) as registros
    from historico as p, unnest(p.historico) as h
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
from historico as p
left join registros as r on p.id_usuario = r.id_usuario
