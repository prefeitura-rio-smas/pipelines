-- Camada Mart: Dimensão Catálogo de Atividades de Grupo (thin)
-- Consome int_atividades_grupo sem transformação (a PK natural é id_atividade).
select *  -- noqa: AM04
from {{ ref('int_atividades_grupo') }}
