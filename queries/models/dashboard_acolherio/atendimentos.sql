{{ config(materialized = 'table') }}

WITH base_atendimentos AS (
  SELECT 
    b.seqatend,
    'u' as modulo,
    b.dtentrada,
    cast(b.horaent as int64) as horaent,
    b.dtsaida,
    b.seqtpatend,
    b.seqprof,
    b.seqpac,
    b.sequs,
    b.datcadast,
    b.indlocalatend,
    b.indatendcanc,
    b.dsclstprof,
    b.seqlogincad,
    f.seqfamil
  FROM {{ source('brutos_acolherio_staging', 'gh_atendimentos') }} b
  LEFT JOIN {{ source('brutos_acolherio_staging', 'gh_familias_membros') }} f ON f.seqpac = b.seqpac

  UNION ALL

  SELECT 
    seqatend,
    'f' as modulo,
    dtentrada,
    cast(horaent as int64) as horaent,
    dtsaida,
    seqtpatend,
    seqprof,
    seqpac,
    sequs,
    datcadast,
    indlocalatend,
    indatendcanc,
    dsclstprof,
    seqlogincad,
    seqfamil
  FROM {{ source('brutos_acolherio_staging', 'gh_atend_familia') }}
),

atendimentos_explodidos AS (
  -- Linhas originais
  SELECT 
    seqatend,
    modulo,
    seqprof,
    dtentrada,
    horaent,
    dtsaida,
    seqtpatend,
    seqpac,
    seqfamil,
    sequs,
    datcadast,
    indlocalatend,
    indatendcanc,
    dsclstprof,
    seqlogincad
  FROM base_atendimentos

  UNION ALL

  -- Linhas explodidas da lista de profissionais
  SELECT 
    seqatend,
    modulo,
    CAST(REGEXP_REPLACE(prof_id, r'^0+', '') AS INT64) AS seqprof,
    dtentrada,
    horaent,
    dtsaida,
    seqtpatend,
    seqpac,
    seqfamil,
    sequs,
    datcadast,
    indlocalatend,
    indatendcanc,
    dsclstprof,
    seqlogincad
  FROM base_atendimentos,
  UNNEST(SPLIT(dsclstprof)) AS prof_id
  WHERE prof_id != ''
),

final as (
SELECT
    e.apus AS CAS,
    CASE
      WHEN e.dscus LIKE 'ALBERGUE%' THEN 'ALBERGUE'
      WHEN e.dscus LIKE 'CRAF%' THEN 'CENTRAL DE RECEPÇÃO'
      WHEN e.dscus LIKE 'CRAS%' THEN 'CRAS'
      WHEN e.dscus LIKE 'CREAS%' THEN 'CREAS'
      WHEN e.dscus LIKE 'CRI%' THEN 'CENTRAL DE RECEPÇÃO'
      WHEN e.dscus LIKE 'REPÚBLICA%' THEN 'REPÚBLICA'
      WHEN e.dscus LIKE 'URS%' THEN 'URS'
      ELSE 'UNIDADE CONVENIADA'
    END AS TIPO_UNIDADE,
    e.dscus AS UNIDADE_ATENDIMENTO,
    e.emailprof AS EMAIL_UNIDADE,
    CASE
      WHEN e.apus = '10' THEN 'cas10@prefeitura.rio'
      WHEN e.apus = '09' THEN 'cas9@prefeitura.rio'
      WHEN e.apus = '08' THEN 'cas8@prefeitura.rio'
      WHEN e.apus = '07' THEN 'cas7@prefeitura.rio'
      WHEN e.apus = '06' THEN 'cas6@prefeitura.rio'
      WHEN e.apus = '05' THEN 'cas5@prefeitura.rio'
      WHEN e.apus = '04' THEN 'cas4@prefeitura.rio'
      WHEN e.apus = '03' THEN 'cas3@prefeitura.rio'
      WHEN e.apus = '02' THEN 'cas2@prefeitura.rio'
      WHEN e.apus = '01' THEN 'cas1@prefeitura.rio'
    END AS EMAIL_CAS,
    b.seqatend AS SEQ_ATENDIMENTO,
    b.dtentrada AS DATA_DE_ATENDIMENTO,
    b.horaent AS HORA_DE_ATENDIMENTO_ORIGINAL,
    FORMAT('%02d:%02d', DIV(b.horaent, 100), MOD(b.horaent, 100)) AS HORA_DE_ATENDIMENTO,
    b.seqpac AS ID_USUARIO,
    b.seqfamil AS ID_FAMILIA,
    p.dscnomepac AS NOME_USUARIO,
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
          )
    END AS NUMERO_DOCUMENTO,
    p.dslogradouro AS ENDERECO,
    p.numend AS ENDERECO_NUMERO,
    p.complend AS ENDERECO_COMPLEMENTO,
    p.dscbairroender AS BAIRRO,
    p.pontorefe AS REFERENCIA_OU_COMUNIDADE,
    c.descatend AS NOME_ATENDIMENTO_ORIGINAL,
    p.datnascim AS DATA_NASCIMENTO,
    date_diff(current_date(), p.datnascim, year) -
                if(extract(dayofyear from p.datnascim) > extract(dayofyear from current_date()), 1, 0) as IDADE,
    CASE
        WHEN p.racacor = '01' THEN 'Branca'
        WHEN p.racacor = '02' THEN 'Preta'
        WHEN p.racacor = '03' THEN 'Parda'
        WHEN p.racacor = '04' THEN 'Amarela'
        WHEN p.racacor = '05' THEN 'Indigena'
        ELSE '99'
    END AS RACA_COR,
    p.indsexo AS SEXO,
    q.seqprof AS PROFISSIONAL_ID,
    trim(upper(q.nomeprof)) AS PROFISSIONAL,
    cb.dsccbo AS PROFISSIONAL_CBO_ORIGINAL,
    CASE
      WHEN cb.dsccbo LIKE 'Articulador Comunitário%' THEN 'Articulador Comunitário'
      WHEN cb.dsccbo LIKE 'Assistente administrativo%' THEN 'Assistente administrativo'
      WHEN cb.dsccbo LIKE 'Assistente Social%' THEN 'Assistente social'
      WHEN cb.dsccbo LIKE 'Assistente social%' THEN 'Assistente social'
      WHEN cb.dsccbo LIKE 'Educador social%' THEN 'Educador social'
      WHEN cb.dsccbo LIKE 'Entrevistador Social%' THEN 'Entrevistador social'
      WHEN cb.dsccbo LIKE 'Pedagogo%' THEN 'Pedagogo'
      WHEN cb.dsccbo LIKE 'Psicólogo%' THEN 'Psicólogo'
      WHEN cb.dsccbo LIKE 'Recepcionista%' THEN 'Recepcionista'
      ELSE cb.dsccbo
    END AS PROFISSIONAL_CBO,
    trim(upper(s.nompess)) AS CADASTRANTE,
    b.datcadast AS DATA_CADASTRO_ATENDIMENTO

FROM atendimentos_explodidos b
JOIN {{ source('brutos_acolherio_staging', 'gh_cidadao_pac') }} p ON p.seqpac = b.seqpac
LEFT JOIN {{ source('brutos_acolherio_staging', 'gh_tpatendimentos') }} c ON c.seqtpatend = b.seqtpatend
LEFT JOIN {{ source('brutos_acolherio_staging', 'gh_us') }} e ON e.sequs = b.sequs
LEFT JOIN {{ source('brutos_acolherio_staging', 'gh_contas') }} s ON s.seqlogin = b.seqlogincad
LEFT JOIN {{ source('brutos_acolherio_staging', 'gh_prof') }} q ON q.seqlogin = s.seqlogin
LEFT JOIN {{ source('brutos_acolherio_staging', 'gh_profocup') }} v ON v.seqprof = q.seqprof
LEFT JOIN {{ source('brutos_acolherio_staging', 'gh_cbo') }} cb ON v.codcbo = cb.codcbo

WHERE
  p.dscnomepac NOT LIKE '%TESTE%' 
  AND e.dscus NOT LIKE '%TESTE%'

ORDER BY 
  e.apus,
  e.dscus,
  b.dtentrada,
  b.horaent
),

filtro_email AS (
  SELECT
  a.*,
  CASE
      WHEN NOME_ATENDIMENTO_ORIGINAL LIKE '%Recepção%' THEN 'Recepção'
      WHEN PROFISSIONAL = 'ATENDIMENTO RECEPÇÃO' THEN 'Recepção'
      WHEN PROFISSIONAL_CBO IN ('Administrador', 'Articulador Comunitário', 'Assistente administrativo', 'Educador social', 'Orientador social', 'Recepcionista')
        AND NOME_ATENDIMENTO_ORIGINAL LIKE '%CadÚnico%'
        THEN 'Recepção'
      WHEN PROFISSIONAL_CBO IN ('Advogado', 'Assistente social', 'Pedagogo', 'Psicólogo') THEN 'Atendimento Técnico'
      ELSE 'Outros Atendimentos'
  END AS TIPO_ATENDIMENTO,
  CASE
      WHEN PROFISSIONAL_CBO IN ('Administrador', 'Articulador Comunitário', 'Assistente administrativo', 'Educador social', 'Orientador social', 'Recepcionista')
        AND TIPO_UNIDADE = 'CRAS'
        AND NOME_ATENDIMENTO_ORIGINAL LIKE '%CadÚnico%'
        THEN 'CRAS - Recepção - Ações CadÚnico'
      WHEN PROFISSIONAL_CBO IN ('Administrador', 'Articulador Comunitário', 'Assistente administrativo', 'Educador social', 'Orientador social', 'Recepcionista')
        AND TIPO_UNIDADE = 'CREAS'
        AND NOME_ATENDIMENTO_ORIGINAL LIKE '%CadÚnico%'
        THEN 'CREAS - Recepção - Ações CadÚnico'
      ELSE NOME_ATENDIMENTO_ORIGINAL
  END AS NOME_ATENDIMENTO,
  CASE
    WHEN a.IDADE < 18 THEN 'Até 17 anos'
    WHEN a.IDADE >= 18 AND a.IDADE < 30 THEN 'De 18 a 29 anos'
    WHEN a.IDADE >= 30 AND a.IDADE < 45 THEN 'De 30 a 44 anos'
    WHEN a.IDADE >= 45 AND a.IDADE < 60 THEN 'De 45 a 59 anos'
    WHEN a.IDADE >= 60 AND a.IDADE < 75 THEN 'De 60 a 74 anos'
    ELSE 'Mais de 75 anos'
  END AS IDADE_FAIXA,
  CONCAT(a.EMAIL_CAS, ',', a.EMAIL_UNIDADE, ',', z.EMAIL) AS EMAIL
  FROM final a
  LEFT JOIN {{ source('dashboard_acolherio', 'filtro_email_dev') }} z ON a.UNIDADE_ATENDIMENTO = z.UNIDADE_ATENDIMENTO
)

SELECT * FROM filtro_email
