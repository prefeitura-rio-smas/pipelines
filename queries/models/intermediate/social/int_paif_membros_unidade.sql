{{ config(materialized = 'ephemeral') }}

-- Famílias PAIF (serviço assistencial 1) em unidades CRAS. Algoritmo de
-- atribuição de unidade em familia_servico_unidade (3 níveis); aqui só o
-- alias legado data_cadastro_paif é preservado para os consumidores.

select
    id_familia,
    data_cadastro_servico as data_cadastro_paif,
    id_usuario,
    data_nascimento,
    beneficio,
    violacoes,
    vulnerabilidades,
    id_unidade
from {{ familia_servico_unidade(1, 'CRAS') }}
