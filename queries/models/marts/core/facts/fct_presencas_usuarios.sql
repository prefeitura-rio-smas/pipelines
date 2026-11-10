-- Camada Mart: Presenças de Usuários em Atividades de Grupo (thin)
-- Consome int_presencas_usuarios e adiciona apenas a surrogate key.
select
    {{ dbt_utils.generate_surrogate_key(['p.id_atividade', 'p.id_usuario', 'p.data_presenca', 'p.hora_presenca']) }} as id_presenca,
    p.*  -- noqa: AM04
from {{ ref('int_presencas_usuarios') }} as p
