-- Camada Intermediate: Presenças de Usuários em Atividades de Grupo
-- Grão: 1 linha por presença de usuário em uma atividade.
-- Input da fct_presencas_usuarios; a surrogate key fica na mart.
-- Nota: INNER JOIN com int_usuarios filtra usuários TESTE, conforme regra de
--       negócio: nenhum TESTE em nenhuma camada downstream.

with presencas as (
    select * from {{ ref('raw_presencas_usuarios') }}
),
usuarios as (
    select id_usuario from {{ ref('int_usuarios') }}
)
select
    p.id_atividade,
    p.id_usuario,
    p.data_presenca,
    p.hora_presenca,
    p.id_unidade,
    p.id_setor
from presencas p
inner join usuarios u on p.id_usuario = u.id_usuario
