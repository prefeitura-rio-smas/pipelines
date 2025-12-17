-- Busca famílias com ACOMPANHAMENTO PAEFI (Mesma query usada no item A bloco I do RMA - CRAS)
WITH base_table AS (
    SELECT
        indativo,
        seqfamil,
        datcancel,
        datcadastr,
        seqlogincad,
        seqservassist,
        seqlogincancel,
        seqfamilservassist,
        EXTRACT(MONTH FROM DATETIME(datcadastr)) AS mes_cadastro
    FROM rj-smas.brutos_acolherio_staging.gh_famil_servassist
    WHERE seqservassist = 1 -- Selecionando o PAIF
    AND datcancel IS NULL -- Cancelamento do acompanhamento PAIF (FALSE)
)

-- Verificando se alguma família que está aparecendo no relatório RMA não aparece na tabela original do PAIF.
SELECT
    DISTINCT(seqfamil)
FROM {{ ref('base_table_bloco1_item_a_cras')}}
WHERE  seqfamil NOT IN (SELECT seqfamil FROM base_table)
AND seqfamil IS NOT NULL


