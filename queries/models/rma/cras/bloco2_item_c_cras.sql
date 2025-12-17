{{ config(materialized='ephemeral') }}

WITH filtro_unidade AS (
    SELECT
        a.seqpac,
        a.sequs,
        a.atividades_smas,
        a.beneficios,
        a.orgaos,
        b.dscus
    FROM {{ ref('base_table_bloco2_item_c_cras')}} a
    LEFT JOIN rj-smas.brutos_acolherio_staging.gh_us b ON a.sequs = b.sequs
),

-- Query que retorna o total de atendimentos individualizados (ITEM C1 - RMA CRAS)
c1 AS (
    SELECT
        unidade_atendimento AS dscus,
        COUNT(seq_atendimento) AS total_atendimentos_C1
    FROM rj-smas-dev.dashboard_acolherio.dev_atendimentos
    GROUP BY unidade_atendimento
),

filtro_c3 AS (
    SELECT
        seqpac,
        dscus,
        orgaos
    FROM filtro_unidade
    WHERE REGEXP_CONTAINS(beneficios, r'Cadastro/Atualização Cadúnico')
),

-- Query para buscar famílias encaminhadas para acesso ao cadunico (ITEM C2/C3 - RMA CRAS)
c3 AS (
    SELECT
        dscus,
        COUNT(seqpac) AS quantidade_encaminhamento_cadunico_C2C3
    FROM filtro_c3
    GROUP BY dscus
),

filtro_c4 AS (
    SELECT
        seqpac,
        dscus,
        orgaos
    FROM filtro_unidade
    WHERE REGEXP_CONTAINS(beneficios, r'BPC - Idoso|BPC - PCD')
),

-- Query para buscar famílias encaminhadas para acesso ao BPC (ITEM C5 - RMA CRAS)
c4 AS (
    SELECT
        dscus,
        COUNT(seqpac) AS quantidade_encaminhamento_bpc_C4
    FROM filtro_c4
    GROUP BY dscus
),


filtro_c5 AS (
    SELECT
        seqpac,
        dscus,
        orgaos
    FROM filtro_unidade
    WHERE REGEXP_CONTAINS(orgaos, r'CREAS')
),

-- Query para buscar famílias encaminhadas para o CREAS (ITEM C5 - RMA CRAS)
c5 AS (
    SELECT
        dscus,
        COUNT(seqpac) AS quantidade_encaminhamento_creas_C5
    FROM filtro_c5
    GROUP BY dscus
),

dscus_all AS (
    SELECT dscus FROM c1
    UNION DISTINCT
    SELECT dscus FROM c3
    UNION DISTINCT 
    SELECT dscus FROM c4
    UNION DISTINCT 
    SELECT dscus FROM c5
)

SELECT
    gh_us.dscus,
    a.total_atendimentos_C1,
    b.quantidade_encaminhamento_cadunico_C2C3,
    c.quantidade_encaminhamento_bpc_C4,
    d.quantidade_encaminhamento_creas_C5
FROM dscus_all gh_us
LEFT JOIN c1 a ON gh_us.dscus = a.dscus
LEFT JOIN c3 b ON gh_us.dscus = b.dscus
LEFT JOIN c4 c ON gh_us.dscus = c.dscus
LEFT JOIN c5 d ON gh_us.dscus = d.dscus