-- Camada Mart: Dimensão Usuários (thin)
-- Consome a int_usuarios (grão: 1 linha por usuário) e adiciona a surrogate key
-- e o id_familia atual (vínculo ativo mais recente via int_familias.membros).
with usuarios as (
    select * from {{ ref('int_usuarios') }}
),

familias as (
    select m.id_usuario, f.id_familia
    from {{ ref('int_familias') }} f,
    unnest(f.membros) as m
    where m.is_atual
)

select
    {{ dbt_utils.generate_surrogate_key(['u.id_usuario']) }} as id_usuario_sk,
    u.*,
    f.id_familia
from usuarios u
left join familias f on u.id_usuario = f.id_usuario
