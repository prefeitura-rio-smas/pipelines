-- Camada Mart: Dimensão Usuários (thin)
-- Consome a int_usuarios (grão: 1 linha por usuário) e adiciona apenas a
-- surrogate key. Toda a lógica de negócio (JOINs, arrays via STRUCT e
-- mapeamentos map_coluna_*) foi movida para a intermediate/core/int_usuarios.
-- Schema de saída idêntico ao anterior: id_usuario_sk (hash do id_usuario
-- natural) seguido de todas as colunas de atributos da int, na mesma ordem.
with usuarios as (
    select * from {{ ref('int_usuarios') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['id_usuario']) }} as id_usuario_sk,
    *
from usuarios
