with source as (
    select
        id_paciente as id_usuario,
        violacao_direito
    from {{ ref('raw_usuarios_detalhes') }}
    where violacao_direito is not null 
      and violacao_direito != ''
      and violacao_direito != 'N'
),

codigos_separados as (
    select
        id_usuario,
        trim(codigo) as codigo
    from source,
    unnest(split(violacao_direito, ',')) as codigo
),

traducao as (
    select
        id_usuario,
        codigo,
        {{ map_violacao_direito_descricao('codigo') }} as descricao
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
        ) as violacoes
    from traducao
    group by id_usuario
)

select * from final
