{% macro pu_filtro_email() %}
    {% set origem = source('acolherio_referencia', 'filtro_email') %}
    {% if execute and var('pu_base_schema', false) %}
        {{ api.Relation.create(database=target.database, schema=var('pu_base_schema'), identifier='filtro_email') }}
    {% else %}{{ origem }}{% endif %}
{% endmacro %}

{% macro pu_bi_input(nome, fontes) %}
    {% if nome in fontes %}{{ fontes[nome] }}{% else %}{{ pu_ref(nome) }}{% endif %}
{% endmacro %}

{% macro pu_atendimentos_bi_sql(fontes={}, filtro_email=none) %}
-- As mesmas partições e expressões da programada, antes de formar os structs.
with fonte_gh_atendimentos as (
    select id_atendimento as seqatend, data_atendimento as dtentrada, hora_atendimento as horaent, data_saida as dtsaida, id_tipo_atendimento as seqtpatend, id_profissional as seqprof, id_paciente as seqpac, id_unidade as sequs, data_cadastro_atendimento as datcadast, local_atendimento as indlocalatend, flag_cancelado as indatendcanc, id_profissional_compartilhado as dsclstprof, id_login_cadastro as seqlogincad
    from {{ pu_bi_input('raw_atendimentos_usuarios', fontes) }}
),
fonte_gh_atend_familia as (
    select id_atendimento as seqatend, data_atendimento as dtentrada, hora_atendimento as horaent, data_saida as dtsaida, id_tipo_atendimento as seqtpatend, id_profissional as seqprof, id_paciente as seqpac, id_unidade as sequs, data_cadastro_atendimento as datcadast, local_atendimento as indlocalatend, flag_cancelado as indatendcanc, id_profissional_compartilhado as dsclstprof, id_login_cadastro as seqlogincad, id_familia as seqfamil
    from {{ pu_bi_input('raw_atendimentos_familias', fontes) }}
),
fonte_gh_familias_membros as (
    select id_paciente as seqpac, id_familia as seqfamil, data_saida as datsaida
    from {{ pu_bi_input('raw_membros_familia', fontes) }}
),
fonte_gh_cidadao_pac as (
    select id_paciente as seqpac, nome as dscnomepac, cpf as numcpfpac, logradouro as dslogradouro, numero_endereco as numend, complemento_endereco as complend, bairro as dscbairroender, ponto_referencia as pontorefe, data_nascimento as datnascim, raca_cor as racacor, sexo as indsexo
    from {{ pu_bi_input('raw_usuarios', fontes) }}
),
fonte_gh_tpatendimentos as (
    select id_tipo_atendimento as seqtpatend, descricao as descatend
    from {{ pu_bi_input('raw_tipos_atendimento', fontes) }}
),
fonte_gh_us as (
    select id_unidade as sequs, nome_unidade as dscus, cas as apus, email_unidade as emailprof
    from {{ pu_bi_input('raw_unidades', fontes) }}
),
fonte_gh_contas as (
    select id_login as seqlogin, nome_operador as nompess
    from {{ pu_bi_input('raw_operadores', fontes) }}
),
fonte_gh_prof as (
    select id_profissional as seqprof, nome as nomeprof
    from {{ pu_bi_input('raw_profissionais', fontes) }}
),
fonte_gh_profocup as (
    select id_profissional as seqprof, codigo_cbo as codcbo
    from {{ pu_bi_input('raw_profissionais_ocupacoes', fontes) }}
),
fonte_gh_cbo as (
    select codigo_cbo as codcbo, descricao as dsccbo
    from {{ pu_bi_input('raw_cbo', fontes) }}
),
fonte_gh_famil_projsociais as (
    select id_familia as seqfamil, id_projeto_social as seqprojsoc, indicador_ativo as indativo, data_cancelamento as datcancel
    from {{ pu_bi_input('raw_familias_projetos_sociais', fontes) }}
),
base_atendimentos AS (
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
    FROM fonte_gh_atendimentos b
    LEFT JOIN fonte_gh_familias_membros f ON f.seqpac = b.seqpac
    where datsaida is null

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
    FROM fonte_gh_atend_familia
  ),

  atendimentos_explodidos AS (
    -- Linhas originais
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

    -- Linhas explodidas da lista de profissionais
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

  final as (
  SELECT
      b.modulo,
      b.sequs as _id_unidade,
      b.seqtpatend as _id_tipo_atendimento,
      b.indatendcanc as _flag_cancelado,
      cb.codcbo as _codigo_cbo,
      e.apus AS CAS,
      CASE
        WHEN e.dscus LIKE 'ALBERGUE%' THEN 'ALBERGUE'
        WHEN e.dscus LIKE 'CRAF%' THEN 'CENTRAL DE RECEPÇÃO'
        WHEN e.dscus LIKE 'CRAS%' THEN 'CRAS'
        WHEN e.dscus LIKE 'CREAS%' THEN 'CREAS'
        WHEN e.dscus LIKE 'CRI%' THEN 'CENTRAL DE RECEPÇÃO'
        WHEN e.dscus LIKE 'REPÚBLICA%' THEN 'REPÚBLICA'
        WHEN e.dscus LIKE 'URS%' THEN 'URS'
        WHEN e.dscus LIKE 'CENTRO POP%' THEN 'CENTRO POP'
        WHEN e.dscus LIKE 'ESCRITÓRIO%' THEN 'ESCRITÓRIO SOCIAL'
        WHEN e.dscus LIKE 'LARES%' THEN 'LARES CARIOCAS'
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
        ELSE 'gdados.smas@prefeitura.rio'
      END AS EMAIL_CAS,
      b.seqatend AS SEQ_ATENDIMENTO,
      b.dtentrada AS DATA_DE_ATENDIMENTO,
      b.horaent AS HORA_DE_ATENDIMENTO_ORIGINAL,
      FORMAT('%02d:%02d', DIV(b.horaent, 100), MOD(b.horaent, 100)) AS HORA_DE_ATENDIMENTO,
      b.seqpac AS ID_USUARIO,
      b.seqfamil AS ID_FAMILIA,
      p.dscnomepac AS NOME_USUARIO,
      CASE WHEN p.numcpfpac IS NULL OR p.numcpfpac = '' THEN 'CPF não informado' ELSE 'CPF informado' END AS DOCUMENTACAO,
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
      trim(regexp_replace(p.dslogradouro, r'[,\s;]+', ' ')) AS ENDERECO,
      trim(regexp_replace(p.numend, r'[,\s;]+', ' ')) AS ENDERECO_NUMERO,
      trim(regexp_replace(p.complend, r'[,\s;]+', ' ')) AS ENDERECO_COMPLEMENTO,
      CASE
        WHEN p.dscbairroender = '' THEN 'NAO INFORMADO'
        ELSE p.dscbairroender
      END AS BAIRRO,
      trim(regexp_replace(p.pontorefe, r'[,\s;]+', ' ')) AS REFERENCIA_OU_COMUNIDADE,
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
          ELSE 'Nao informado'
      END AS RACA_COR,
      p.indsexo AS SEXO,
      q.seqprof AS PROFISSIONAL_ID,

      CASE
        WHEN trim(upper(q.nomeprof)) IN ('ATENDIMENTO RECEPÇÃO', 'ATENDIMENTO BUSCA ATIVA', 'ATENDIMENTO ENTREVISTADOR SOCIAL') THEN trim(upper(s.nompess)) ELSE trim(upper(q.nomeprof))
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
      trim(upper(s.nompess)) AS CADASTRANTE,
      b.datcadast AS DATA_CADASTRO_ATENDIMENTO

  FROM atendimentos_explodidos b
  JOIN fonte_gh_cidadao_pac p ON p.seqpac = b.seqpac
  LEFT JOIN fonte_gh_tpatendimentos c ON c.seqtpatend = b.seqtpatend
  LEFT JOIN fonte_gh_us e ON e.sequs = b.sequs
  LEFT JOIN fonte_gh_contas s ON s.seqlogin = b.seqlogincad
  LEFT JOIN fonte_gh_prof q ON q.seqprof = b.seqprof
  LEFT JOIN fonte_gh_profocup v ON v.seqprof = q.seqprof
  LEFT JOIN fonte_gh_cbo cb ON v.codcbo = cb.codcbo

  WHERE
    s.nompess NOT LIKE '%TESTE%'
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
        WHEN NOME_ATENDIMENTO_ORIGINAL LIKE '%Recepção%' THEN 'Atendimento Recepção'
        WHEN PROFISSIONAL = 'ATENDIMENTO RECEPÇÃO' THEN 'Atendimento Recepção'
        --CORRECAO DE REGISTROS FEITOS EM SETEMBRO/2025 (QUANDO APARECIAM MUITAS OPÇÕES DE ATENDIMENTO DE CADUNICO PARA PNM)
        WHEN PROFISSIONAL_CBO IN ('Administrador', 'Articulador Comunitário', 'Assistente administrativo', 'Educador social', 'Orientador social', 'Recepcionista')
         AND NOME_ATENDIMENTO_ORIGINAL LIKE '%CadÚnico%'
         THEN 'Atendimento Recepção'
        --FIM DA CORRECAO DE REGISTROS FEITOS EM SETEMBRO/2025
        WHEN PROFISSIONAL_CBO IN ('Advogado', 'Assistente social', 'Pedagogo', 'Psicólogo') THEN 'Atendimento Técnico' -- alinhado com SUBPSB em 15/01/2026
        ELSE 'Outros Atendimentos'
    END AS TIPO_ATENDIMENTO,
    CASE
        WHEN PROFISSIONAL_CBO IN ('Administrador', 'Articulador Comunitário', 'Assistente administrativo', 'Educador social', 'Orientador social', 'Recepcionista')
         AND TIPO_UNIDADE = 'CRAS'
         AND NOME_ATENDIMENTO_ORIGINAL LIKE '%CadÚnico%'
         THEN 'CRAS - Recepção - Ação CadÚnico'
        WHEN PROFISSIONAL_CBO IN ('Administrador', 'Articulador Comunitário', 'Assistente administrativo', 'Educador social', 'Orientador social', 'Recepcionista')
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
    LEFT JOIN {{ filtro_email if filtro_email is not none else pu_filtro_email() }} z ON a.UNIDADE_ATENDIMENTO = z.UNIDADE_ATENDIMENTO
  ),

catalogo_projetos_sociais AS (
  SELECT *
  FROM UNNEST(ARRAY<STRUCT<id_projeto_social INT64, nome_projeto_social STRING>>[
      (1, 'Territórios Sociais'),
      (2, 'Pequenos Cariocas'),
      (3, 'MCMV - Minha Casa Minha Vida'),
      (4, 'Alimenta Rio'),
      (5, 'Banco de Alimentos/Cozinha Comunitária'),
      (6, 'Banco Carioca de Bolsa de Estudos'),
      (7, 'Projetos de Empreendedorismo'),
      (8, 'PRONASCI Juventude'),
      (9, 'Agente Experiente'),
      (10, 'Amparando Filhos'),
      (11, 'Bora pra Escola'),
      (12, 'DAM+'),
      (13, 'De Mãos Dadas'),
      (14, 'De Volta a Terra Natal'),
      (15, 'Escritório Social'),
      (16, 'Garupa'),
      (17, 'Guarda Subsidiada'),
      (18, 'Jovem Aprendiz'),
      (19, 'Lares Cariocas'),
      (20, 'Moradia com Apoio'),
      (21, 'Novos Rumos'),
      (22, 'Passo a Passo'),
      (23, 'Penas e Medidas Alternativas'),
      (24, 'PETI - Errad. Trab. Infantil'),
      (25, 'Projetos de Qualificação Profissional'),
      (26, 'Reinserção familiar ou comunitária'),
      (27, 'SUAS Acolhendo Talentos'),
      (28, 'Vem com a gente'),
      (29, 'Rio Dignidade'),
      (30, 'Idoso em Família'),
      (31, 'Projovem Urbano'),
      (32, 'Tô de Boa')
  ])
),

-- Situação atual do cadastro familiar, inclusive para atendimentos antigos.
-- Um registro pode continuar com indativo = 'S' após o cancelamento.
projetos_sociais_por_familia AS (
  SELECT
    vinculo.seqfamil AS id_familia,
    STRING_AGG(
      DISTINCT COALESCE(
        catalogo.nome_projeto_social,
        FORMAT('Projeto não mapeado (ID %d)', vinculo.seqprojsoc)
      ),
      ', ' ORDER BY COALESCE(
        catalogo.nome_projeto_social,
        FORMAT('Projeto não mapeado (ID %d)', vinculo.seqprojsoc)
      )
    ) AS PROJETOS_SOCIAIS
  FROM fonte_gh_famil_projsociais AS vinculo
  LEFT JOIN catalogo_projetos_sociais AS catalogo
    ON catalogo.id_projeto_social = vinculo.seqprojsoc
  WHERE vinculo.indativo = 'S'
    AND vinculo.datcancel IS NULL
  GROUP BY vinculo.seqfamil
),

base as (
  select distinct * from filtro_email
)

select base.*,
row_number () over (partition by ID_ATENDIMENTO order by base.DATA_CADASTRO_ATENDIMENTO asc nulls first, base.modulo asc nulls first, base.SEQ_ATENDIMENTO asc nulls first, base.ID_USUARIO asc nulls first, base.ID_FAMILIA asc nulls first, base._codigo_cbo asc nulls first, base.PROFISSIONAL_ID asc nulls first, to_json_string((select as struct base.*))) as cbo_unico_rank, -- resolve problema de duplicada de atendimento quando o profissional tem 2 cbo`s associados
row_number() over (partition by profissional_id, hora_de_atendimento, data_de_atendimento, NOME_ATENDIMENTO_ORIGINAL, id_usuario, unidade_atendimento order by base.DATA_CADASTRO_ATENDIMENTO asc nulls first, base.modulo asc nulls first, base.SEQ_ATENDIMENTO asc nulls first, base.ID_USUARIO asc nulls first, base.ID_FAMILIA asc nulls first, base._codigo_cbo asc nulls first, base.PROFISSIONAL_ID asc nulls first, to_json_string((select as struct base.*))) as atendimento_unico_rank, -- rank do agrupamento de atendimentos que tem todos os itens listados em comum, i.e. atendimentos repetidos
  projetos.PROJETOS_SOCIAIS
 from base
LEFT JOIN projetos_sociais_por_familia AS projetos
  ON projetos.id_familia = base.ID_FAMILIA
qualify cbo_unico_rank = 1  -- filtrando apenas pelo primeiro cbo
and atendimento_unico_rank = 1 -- filtrando apenas por atendimentos nao repetidos


{% endmacro %}
