{{ config(materialized='table') }}

with violacoes_explodidas as (
    select
        iv.id_usuario,
        v.codigo,
        v.descricao
    from {{ ref('int_usuarios_violacoes') }} as iv
    cross join unnest(iv.violacoes) as v
    where v.origem = 'checkbox'
),

joined as (
    select
        du.id_usuario_sk as sk_usuario,
        ve.id_usuario,
        du.nome as nome_usuario,
        du.cpf,
        du.data_nascimento,
        ve.codigo as codigo_violacao,
        ve.descricao as descricao_violacao,
        {{ extrair_ultima_atualizacao('raw_configuracoes_sistema') }} as ultima_atualizacao
    from violacoes_explodidas as ve
    left join {{ ref('dim_usuarios') }} as du
        on ve.id_usuario = du.id_usuario
)

select * from joined
