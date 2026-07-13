-- Fato: Presenças de Usuários em Atividades de Grupo
-- Grão: 1 linha por presença de usuário em uma atividade
-- Nota: INNER JOIN com dim_usuarios filtra usuários TESTE (nome LIKE '%TESTE%')
--       conforme regra de negócio: nenhum TESTE em nenhuma camada downstream.

with presencas as (
    select * from {{ ref('raw_presencas_usuarios') }}
),
usuarios as (
    select id_usuario from {{ ref('dim_usuarios') }}
)
select
    {{ dbt_utils.generate_surrogate_key(['p.id_atividade', 'p.id_usuario', 'p.data_presenca', 'p.hora_presenca']) }} as id_presenca,
    p.id_atividade,
    p.id_usuario,
    p.data_presenca,
    p.hora_presenca,
    p.id_unidade,
    p.id_setor
from presencas p
inner join usuarios u on p.id_usuario = u.id_usuario
