{{ config(materialized = 'table') }}

WITH acolhimentos AS (
    SELECT 
    rg.seqpac,
    pac.seqciclo,
    us.dscus AS unidade,
    rg.seqpac as id_usuario,
    pac.sequs,
    rg.nome_usuario,
    rg.nome_social,
    rg.prontuario,
    rg.cpf,
    rg.filiacao_mae as filiacao,
    rg.data_nascimento,
    pac.dtentrada AS DATA_ENTRADA,
    pac.dtsaida AS DATA_DESLIGAMENTO,
    {{ map_coluna_motivo_desligamento('pac.indmotivsaida') }} 
    FROM {{ ref('mart_usuarios_acolherio') }} rg
    LEFT JOIN {{ source('brutos_acolherio_staging', 'gh_pac_ciclos')}} pac ON pac.seqpac = rg.seqpac
    LEFT JOIN {{ source('brutos_acolherio_staging', 'gh_us')}} us ON us.sequs = pac.sequs
)

SELECT * FROM acolhimentos
