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
),

-- Busca todos os membros de cada família que está no ACOMPANHAMENTO PAEFI
membro_familia  AS (
    SELECT 
        a.seqfamil,
        b.seqmembro,
        a.mes_cadastro,
        b.seqpac,
        b.datsaida
    from base_table a 
    LEFT JOIN rj-smas.brutos_acolherio_staging.gh_familias_membros b ON a.seqfamil = b.seqfamil
),

filtro_mes AS (
SELECT 
    * 
FROM base_table
WHERE mes_cadastro = 11
),

tipo_beneficios AS (
  SELECT 
    id_usuario,
    beneficio
  FROM rj-smas-dev.gerenciamento__dbt.tipo_beneficio
  WHERE id_usuario IS NOT NULL
),

usuarios_bolsa_familia AS (
    SELECT
        a.id_usuario,
        a.beneficio,
        b.seqmembro,
        b.seqfamil
    FROM tipo_beneficios a
    INNER JOIN membro_familia b ON a.id_usuario = b.seqpac
    WHERE seqmembro = 1
)

SELECT * FROM usuarios_bolsa_familia