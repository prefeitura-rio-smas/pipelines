
with criar_id_usuario_repetidos_membros_familia as (
    select 
    *, 
    row_number() over(
    partition  by seqpac order by seqpac desc
    ) as n_usuario
    from {{ ref('stg_membros_familia_acolherio') }}
    where data_saida_membro_familia is null
),

usuarios_unicos_membro_familia as (
    select
        *
    from criar_id_usuario_repetidos_membros_familia
    where n_usuario = 1
)

select * from usuarios_unicos_membro_familia