-- Materialização de dev_atendimentos para o Looker.
-- A fonte é atualizada pelo Prefect somente após a checagem de frescor das duas
-- tabelas de atendimento do AcolheRio.

CREATE OR REPLACE TABLE `rj-smas-dev.dashboard_acolherio.dev_atendimentos` AS (
  WITH base_atendimentos AS (
    SELECT
      b.seqatend,
      'u' AS modulo,
      b.dtentrada,
      CAST(b.horaent AS INT64) AS horaent,
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
    FROM `rj-smas.brutos_acolherio_staging.gh_atendimentos` b
    LEFT JOIN `rj-smas.brutos_acolherio_staging.gh_familias_membros` f
      ON f.seqpac = b.seqpac
    WHERE b.dtsaida IS NULL

    UNION ALL

    SELECT
      seqatend,
      'f' AS modulo,
      dtentrada,
      CAST(horaent AS INT64) AS horaent,
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
    FROM `rj-smas.brutos_acolherio_staging.gh_atend_familia`
  ),

  atendimentos_explodidos AS (
    SELECT
      seqatend,
      modulo,
      SAFE_CAST(TRIM(CAST(seqprof AS STRING)) AS INT64) AS seqprof,
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

    SELECT
      seqatend,
      modulo,
      SAFE_CAST(TRIM(REGEXP_REPLACE(prof_id, r'^0+', '')) AS INT64) AS seqprof,
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

  final AS (
    SELECT
      b.modulo,
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
      FORMAT('%02d:%02d', DIV(b.horaent, 100), MOD(b.horaent, 100))
        AS HORA_DE_ATENDIMENTO,
      b.seqpac AS ID_USUARIO,
      b.seqfamil AS ID_FAMILIA,
      p.dscnomepac AS NOME_USUARIO,
      CASE
        WHEN p.numcpfpac IS NULL OR p.numcpfpac = '' THEN 'CPF não informado'
        ELSE 'CPF informado'
      END AS DOCUMENTACAO,
      CASE
        WHEN p.numcpfpac IS NULL OR p.numcpfpac = '' THEN NULL
        ELSE CONCAT(
          SUBSTRING(LPAD(p.numcpfpac, 11, '0'), 1, 3), '.',
          SUBSTRING(LPAD(p.numcpfpac, 11, '0'), 4, 3), '.',
          SUBSTRING(LPAD(p.numcpfpac, 11, '0'), 7, 3), '-',
          SUBSTRING(LPAD(p.numcpfpac, 11, '0'), 10, 2)
        )
      END AS NUMERO_DOCUMENTO,
      TRIM(REGEXP_REPLACE(p.dslogradouro, r'[,\s;]+', ' ')) AS ENDERECO,
      TRIM(REGEXP_REPLACE(p.numend, r'[,\s;]+', ' ')) AS ENDERECO_NUMERO,
      TRIM(REGEXP_REPLACE(p.complend, r'[,\s;]+', ' '))
        AS ENDERECO_COMPLEMENTO,
      CASE
        WHEN p.dscbairroender = '' THEN 'NAO INFORMADO'
        ELSE p.dscbairroender
      END AS BAIRRO,
      TRIM(REGEXP_REPLACE(p.pontorefe, r'[,\s;]+', ' '))
        AS REFERENCIA_OU_COMUNIDADE,
      c.descatend AS NOME_ATENDIMENTO_ORIGINAL,
      p.datnascim AS DATA_NASCIMENTO,
      DATE_DIFF(CURRENT_DATE(), p.datnascim, YEAR)
        - IF(
            EXTRACT(DAYOFYEAR FROM p.datnascim)
              > EXTRACT(DAYOFYEAR FROM CURRENT_DATE()),
            1,
            0
          ) AS IDADE,
      CASE
        WHEN p.racacor = '01' THEN 'Branca'
        WHEN p.racacor = '02' THEN 'Preta'
        WHEN p.racacor = '03' THEN 'Parda'
        WHEN p.racacor = '04' THEN 'Amarela'
        WHEN p.racacor = '05' THEN 'Indigena'
        ELSE 'Nao informado'
      END AS RACA_COR,
      p.indsexo AS SEXO,
      q.seqprof AS PROFISSIONAL_ID,
      CASE
        WHEN TRIM(UPPER(q.nomeprof)) IN (
          'ATENDIMENTO RECEPÇÃO',
          'ATENDIMENTO BUSCA ATIVA',
          'ATENDIMENTO ENTREVISTADOR SOCIAL'
        ) THEN TRIM(UPPER(s.nompess))
        ELSE TRIM(UPPER(q.nomeprof))
      END AS PROFISSIONAL,
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
      TRIM(UPPER(s.nompess)) AS CADASTRANTE,
      b.datcadast AS DATA_CADASTRO_ATENDIMENTO
    FROM atendimentos_explodidos b
    JOIN `rj-smas.brutos_acolherio_staging.gh_cidadao_pac` p
      ON p.seqpac = b.seqpac
    LEFT JOIN `rj-smas.brutos_acolherio_staging.gh_tpatendimentos` c
      ON c.seqtpatend = b.seqtpatend
    LEFT JOIN `rj-smas.brutos_acolherio_staging.gh_us` e
      ON e.sequs = b.sequs
    LEFT JOIN `rj-smas.brutos_acolherio_staging.gh_contas` s
      ON s.seqlogin = b.seqlogincad
    LEFT JOIN `rj-smas.brutos_acolherio_staging.gh_prof` q
      ON q.seqprof = b.seqprof
    LEFT JOIN `rj-smas.brutos_acolherio_staging.gh_profocup` v
      ON v.seqprof = q.seqprof
    LEFT JOIN `rj-smas.brutos_acolherio_staging.gh_cbo` cb
      ON v.codcbo = cb.codcbo
    WHERE s.nompess NOT LIKE '%TESTE%'
      AND p.dscnomepac NOT LIKE '%TESTE%'
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
        WHEN NOME_ATENDIMENTO_ORIGINAL LIKE '%Recepção%'
          THEN 'Atendimento Recepção'
        WHEN PROFISSIONAL = 'ATENDIMENTO RECEPÇÃO'
          THEN 'Atendimento Recepção'
        -- Correção de registros feitos em setembro/2025 para CadÚnico.
        WHEN PROFISSIONAL_CBO IN (
          'Administrador',
          'Articulador Comunitário',
          'Assistente administrativo',
          'Educador social',
          'Orientador social',
          'Recepcionista'
        )
          AND NOME_ATENDIMENTO_ORIGINAL LIKE '%CadÚnico%'
          THEN 'Atendimento Recepção'
        WHEN PROFISSIONAL_CBO IN (
          'Advogado',
          'Assistente social',
          'Pedagogo',
          'Psicólogo'
        )
          THEN 'Atendimento Técnico'
        ELSE 'Outros Atendimentos'
      END AS TIPO_ATENDIMENTO,
      CASE
        WHEN PROFISSIONAL_CBO IN (
          'Administrador',
          'Articulador Comunitário',
          'Assistente administrativo',
          'Educador social',
          'Orientador social',
          'Recepcionista'
        )
          AND TIPO_UNIDADE = 'CRAS'
          AND NOME_ATENDIMENTO_ORIGINAL LIKE '%CadÚnico%'
          THEN 'CRAS - Recepção - Ação CadÚnico'
        WHEN PROFISSIONAL_CBO IN (
          'Administrador',
          'Articulador Comunitário',
          'Assistente administrativo',
          'Educador social',
          'Orientador social',
          'Recepcionista'
        )
          AND TIPO_UNIDADE = 'CREAS'
          AND NOME_ATENDIMENTO_ORIGINAL LIKE '%CadÚnico%'
          THEN 'CREAS - Recepção - Ação CadÚnico'
        ELSE NOME_ATENDIMENTO_ORIGINAL
      END AS NOME_ATENDIMENTO,
      CASE
        WHEN a.IDADE < 18 THEN 'Até 17 anos'
        WHEN a.IDADE >= 18 AND a.IDADE < 30 THEN 'De 18 a 29 anos'
        WHEN a.IDADE >= 30 AND a.IDADE < 45 THEN 'De 30 a 44 anos'
        WHEN a.IDADE >= 45 AND a.IDADE < 60 THEN 'De 45 a 59 anos'
        WHEN a.IDADE >= 60 AND a.IDADE < 75 THEN 'De 60 a 74 anos'
        WHEN a.IDADE >= 75 THEN 'Mais de 75 anos'
        ELSE 'Não Informado'
      END AS IDADE_FAIXA,
      CONCAT(SEQ_ATENDIMENTO, '-', PROFISSIONAL_ID) AS ID_ATENDIMENTO,
      CONCAT(a.EMAIL_CAS, ',', a.EMAIL_UNIDADE, ',', z.EMAIL) AS EMAIL
    FROM final a
    LEFT JOIN `rj-smas-dev.dashboard_acolherio.filtro_email_dev` z
      ON a.UNIDADE_ATENDIMENTO = z.UNIDADE_ATENDIMENTO
  ),

  base AS (
    SELECT *
    FROM filtro_email
  )

  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY ID_ATENDIMENTO
      ORDER BY DATA_CADASTRO_ATENDIMENTO
    ) AS cbo_unico_rank,
    ROW_NUMBER() OVER (
      PARTITION BY
        PROFISSIONAL_ID,
        HORA_DE_ATENDIMENTO,
        DATA_DE_ATENDIMENTO,
        NOME_ATENDIMENTO_ORIGINAL,
        ID_USUARIO,
        UNIDADE_ATENDIMENTO
    ) AS atendimento_unico_rank
  FROM base
  QUALIFY cbo_unico_rank = 1
    AND atendimento_unico_rank = 1
);

