-- Camada Mart: Dimensão Unidades (thin)
-- Consome int_unidades e adiciona apenas a surrogate key.
select
    {{ dbt_utils.generate_surrogate_key(['u.id_unidade']) }} as id_unidade_sk,
    u.*  -- noqa: AM04
from {{ ref('int_unidades') }} as u
