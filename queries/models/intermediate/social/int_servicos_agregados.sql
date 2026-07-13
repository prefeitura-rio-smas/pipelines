with base as (
    select * from {{ ref('raw_familias_servicos_assistenciais') }}
),
agregado as (
    select
        id_familia,
        array_agg(
            struct(
                id_servico_assistencial,
                data_cadastro,
                id_login_cadastro as id_profissional
            )
        ) as servicos
    from base
    group by id_familia
)
select * from agregado
