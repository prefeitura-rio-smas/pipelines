{{ config(materialized = 'ephemeral') }}

with base as (
    select
        id_usuario_sk,
        tipo_beneficio
    from {{ ref('dim_usuarios') }}
    where tipo_beneficio is not null
      and tipo_beneficio != ''
      and upper(trim(tipo_beneficio)) != 'N'
),

codigos_separados as (
    select
        id_usuario_sk,
        trim(codigo) as codigo
    from base,
    unnest(split(tipo_beneficio, ',')) as codigo
),

final as (
    select
        id_usuario_sk,
        {{ map_coluna_beneficio('codigo') }} as beneficio_label
    from codigos_separados
    where codigo != ''
)

select * from final
