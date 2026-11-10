-- Camada Mart: Presenças de Profissionais em Atividades de Grupo (thin)
-- Consome int_presencas_profissionais e adiciona apenas a surrogate key.
select
    {{ dbt_utils.generate_surrogate_key(['p.id_atividade', 'p.id_profissional', 'p.data_presenca', 'p.hora_presenca']) }} as id_presenca,
    p.*  -- noqa: AM04
from {{ ref('int_presencas_profissionais') }} as p
