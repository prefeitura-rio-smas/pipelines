{{ config(materialized = 'ephemeral') }}

with base as (
    select
        id_usuario_sk,
        tipo_deficiencia
    from {{ ref('dim_usuarios') }}
    where tipo_deficiencia is not null
      and tipo_deficiencia != ''
      and upper(trim(tipo_deficiencia)) != 'N'
),

codigos_separados as (
    select
        id_usuario_sk,
        trim(codigo) as codigo
    from base,
    unnest(split(tipo_deficiencia, ',')) as codigo
),

final as (
    select
        id_usuario_sk,
        {{ map_coluna_tipo_deficiencia('codigo') }} as deficiencia_label
    from codigos_separados
    where codigo != ''
)

select * from final
