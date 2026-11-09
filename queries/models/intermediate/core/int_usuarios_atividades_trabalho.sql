{{ config(materialized='ephemeral') }}
with itens as (
    select
        d.id_paciente,
        trim(codigo) as codigo
    from {{ ref('raw_usuarios_detalhes') }} as d,
        unnest(split(coalesce(d.tipos_atividade_trabalho, ''), ',')) as codigo
    where nullif(trim(codigo), '') is not null
)

select
    i.id_paciente,
    string_agg(
        distinct coalesce(c.descricao, concat('Atividade não mapeada (código ', i.codigo, ')')), ', '
        order by coalesce(c.descricao, concat('Atividade não mapeada (código ', i.codigo, ')'))
    ) as tipo_atividade_remunerada
from itens as i
left join {{ ref('acolherio_atividades_trabalho') }} as c on i.codigo = cast(c.codigo as string)
group by i.id_paciente
