{{ config(materialized='table') }}

with violacoes_explodidas as (
    select
        iv.id_usuario,
        v.codigo,
        v.descricao
    from {{ ref('int_usuarios_violacoes') }} iv
    cross join unnest(iv.violacoes) as v
),

joined as (
    select
        du.id_usuario_sk as sk_usuario,
        ve.id_usuario,
        du.nome as nome_usuario,
        du.cpf,
        du.data_nascimento,
        ve.codigo as codigo_violacao,
        ve.descricao as descricao_violacao
    from violacoes_explodidas ve
    left join {{ ref('dim_usuarios') }} du
        on ve.id_usuario = du.id_usuario
)

select * from joined
