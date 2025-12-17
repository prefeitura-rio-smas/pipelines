-- Teste para verificar se há alguma família em acompanhamento PAIF que não foi incluida no coluna (ITEM A1)
SELECT
    seqfamil
FROM {{ ref('base_filtro_bloco1_item_a_v2')}}
WHERE seqfamil NOT IN (
    SELECT 
        seqfamil     
    FROM {{ source('brutos_acolherio_staging', 'gh_famil_servassist') }}
    WHERE seqfamil IS NOT NULL
    AND datcancel IS NULL
    AND seqservassist = 1
)