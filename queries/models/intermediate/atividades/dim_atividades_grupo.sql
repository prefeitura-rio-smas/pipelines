-- Dimensão: Catálogo de Atividades de Grupo
-- Grão: 1 linha por atividade de grupo

with atividades as (
    select * from {{ ref('raw_atividades_grupo') }}
),
tipos as (
    select * from {{ ref('raw_tipos_atividade') }}
),
unidades as (
    select * from {{ ref('dim_unidades') }}
)
select
    a.id_atividade,
    a.nome_atividade,
    t.nome_tipo_atividade,
    t.id_tipo_atividade,
    u.id_unidade,
    u.nome_unidade,
    a.data_inicio,
    a.data_fim,
    a.hora_inicio,
    a.hora_fim,
    a.recorrencia,
    a.indicador_matricula,
    a.observacao_matricula,
    a.id_local
from atividades a
left join tipos t on a.id_tipo_atividade = t.id_tipo_atividade
left join unidades u on a.id_unidade = u.id_unidade
