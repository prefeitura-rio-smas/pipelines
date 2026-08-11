{{ config(materialized = 'ephemeral') }}

with base as (
    select
        id_paciente as id_usuario,
        tipo_deficiencia
    from {{ ref('raw_usuarios_saude_mental') }}
    where tipo_deficiencia is not null
      and tipo_deficiencia != ''
      and upper(trim(tipo_deficiencia)) != 'N'
),

codigos_separados as (
    select
        id_usuario,
        trim(codigo) as codigo
    from base,
    unnest(split(tipo_deficiencia, ',')) as codigo
),

traducao as (
    select
        id_usuario,
        codigo,
        {{ map_coluna_tipo_deficiencia('codigo') }} as descricao
    from codigos_separados
    where codigo != ''
),

final as (
    select
        id_usuario,
        array_agg(
            struct(
                codigo,
                descricao
             ) 
          ) as deficiencia
        from traducao
        group by id_usuario
)

select * from final