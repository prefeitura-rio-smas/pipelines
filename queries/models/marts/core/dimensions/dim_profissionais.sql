-- Camada Mart: Dimensão Profissionais (thin)
-- Consome int_profissionais e adiciona apenas a surrogate key.
select
    {{ dbt_utils.generate_surrogate_key(['p.id_profissional']) }} as id_profissional_sk,
    p.*  -- noqa: AM04
from {{ ref('int_profissionais') }} as p
