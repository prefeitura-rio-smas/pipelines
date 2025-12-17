{{ config(materialized='ephemeral') }}

/*
Query para o bloco I item I1
*/

-- Tabela para consultar pessoas em situação de rua que ingressaram no ACOMPANHAMENTO PAEFI.
WITH filtro_situacao_rua AS (
    SELECT
        seqfamil,
        seqmembro,
        seqpac,
        dscus,
        idade,
        situacao_de_rua
    FROM {{ ref('base_table_bloco1_item_c')}}
    WHERE situacao_de_rua  = 'S'
)

-- Query com o total por unidade de pessoas em situação de rua que ingressaram no ACOMPANHAMENTO PAEFI (ITEM I1 - RMA CREAS)
SELECT
    seqpac,
    dscus,
    COUNT(*) AS situacao_de_rua
FROM filtro_situacao_rua
GROUP BY dscus, seqpac
