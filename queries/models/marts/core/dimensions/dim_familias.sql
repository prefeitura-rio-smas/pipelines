-- Camada Mart: Dimensão Famílias (thin)
-- Consome int_familias e adiciona apenas a surrogate key.
with familias as (
    select * from {{ ref('int_familias') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['id_familia']) }} as id_familia_sk,
    id_familia,
    id_usuario_responsavel,
    nome_responsavel,
    data_ultima_modificacao,
    flag_ativo,
    vulnerabilidades,
    servicos,
    projetos_sociais,
    membros
from familias
