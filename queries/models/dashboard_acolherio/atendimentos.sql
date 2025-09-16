{{ config(materialized = 'table') }}

SELECT
    e.dscus              AS UNIDADE_ATENDIMENTO,
    b.seqatend           AS SEQ_ATENDIMENTO,
    b.dtentrada          AS DATA_DE_ATENDIMENTO,
    p.dscnomepac         AS NOME_USUARIO,
    CASE WHEN p.numcpfpac IS NULL OR p.numcpfpac = '' THEN '' ELSE "CPF" END AS DOCUMENTACAO,
    CASE
       WHEN p.numcpfpac IS NULL OR p.numcpfpac = ''
       THEN NULL
       ELSE
           CONCAT(
               SUBSTRING(LPAD(p.numcpfpac, 11, '0'), 1, 3), '.',
               SUBSTRING(LPAD(p.numcpfpac, 11, '0'), 4, 3), '.',
               SUBSTRING(LPAD(p.numcpfpac, 11, '0'), 7, 3), '-',
               SUBSTRING(LPAD(p.numcpfpac, 11, '0'), 10, 2)
    )END AS NUMERO_DOCUMENTO,
    p.dslogradouro       AS ENDERECO,
    p.numend             AS ENDERECO_NUMERO,
    p.complend           AS ENDERECO_COMPLEMENTO,
    p.dscbairroender     AS BAIRRO,
    p.pontorefe          AS REFERENCIA_OU_COMUNIDADE,
    c.descatend          AS NOME_ATENDIMENTO,
    p.datnascim          AS DATA_NASCIMENTO,
    TIMESTAMP_DIFF(CURRENT_DATE(), p.datnascim, YEAR) AS IDADE,
    CASE
        WHEN p.racacor = '01' THEN 'Branca'
        WHEN p.racacor = '02' THEN 'Preta'
        WHEN p.racacor = '03' THEN 'Parda'
        WHEN p.racacor = '04' THEN 'Amarela'
        WHEN p.racacor = '05' THEN 'Indigena'
        ELSE '99'
    END AS RACA_COR,
    p.indsexo            AS SEXO
FROM {{ source('brutos_acolherio_staging', 'gh_atendimentos') }} b
JOIN {{ source('brutos_acolherio_staging', 'gh_cidadao_pac') }} p ON p.seqpac = b.seqpac
LEFT JOIN {{ source('brutos_acolherio_staging', 'gh_tpatendimentos') }} c ON c.seqtpatend = b.seqtpatend
LEFT JOIN {{ source('brutos_acolherio_staging', 'gh_us') }}          e ON e.sequs      = b.sequs