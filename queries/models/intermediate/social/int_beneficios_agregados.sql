{{ config(materialized = 'ephemeral') }}

with base as (
    select
        id_paciente as id_usuario,
        tipo_beneficio
    from {{ ref('raw_usuarios_saude_mental') }}
    where tipo_beneficio is not null
      and tipo_beneficio != ''
      and upper(trim(tipo_beneficio)) != 'N'
),

codigos_separados as (
    select
        id_usuario,
        trim(codigo) as codigo
    from base,
    unnest(split(tipo_beneficio, ',')) as codigo
),

traducao as (
    select
        id_usuario,
        codigo,
        {{ map_coluna_beneficio('codigo') }} as descricao
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
          ) as beneficio
        from traducao
        group by id_usuario
)

select * from final