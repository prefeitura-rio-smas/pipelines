{{ config(materialized = 'table') }}

WITH usuario_cadastrado AS (
  SELECT
  fam.seqfamil AS ID_FAMILIA,
  pac.dscnomepac AS NOME_USUARIO,
  pac.dscnomsoci AS NOME_SOCIAL,
  pac.datnascim AS DATA_NASCIMENTO,
  {{ map_coluna_nacionalidade('pac.nacional') }},
  {{ map_coluna_cond_estrangeiro('pac.condestr') }},
  pac.paisorigem AS PAIS_ORIGEM,
  pac.dscbairroender AS BAIRRO,
  {{ map_coluna_etnia('pac.racacor') }},
  pac.numcpfpac AS CPF,
  pac.indsexo AS SEXO,
  {{ map_coluna_genero('pac.indgenero') }},
  dados.indorientsex AS ORIENTACAO_SEXUAL,
  {{ map_coluna_filiacao('pac.estcivil') }},
  ori.dscoripcsm AS ORIGEM_DEMANDA,
  pac.nuprontpapel AS PRONTUARIO,
  pac.datcadast AS DATA_CADASTRAMENTO,
  pac.seqlogin AS CADASTRANTE,
  sm.indtrab AS TRABALHA,
  sm.nmprofi AS PROFISSAO,
  sm.indtrab AS ATVD_REMUNERADA,
  sm.dsctipovinc AS TIPO_VINCULO,
  sm.indfreqescol AS FREQ_ESCOLA,
  {{ map_coluna_escolaridade('sm.indserie') }},
  -- pac.indcadunico AS CADUNICO,
  sm.indrecbenef AS RENDA_BENEF,
  sm.indqualbenef AS BENEFICIO,
  sm.indcuratela AS CURATELA,
  {{ map_coluna_tipo_curatela('sm.indtipcuratela') }},
  {{ map_coluna_decisao_apoiada('dados.dsctomdecproces') }},
  dados.dsctomdecnome AS NOME_APOIADOR,
  {{ map_coluna_saude_mental('dados.indsmentcompr') }},
  sm.indpresdefi AS DEFICIENCIA,
  sm.indtpdefi AS TIPO_DEFICIENCIA,
  {{ map_coluna_situacao_de_rua('sm.indmoradi') }},
  {{ map_coluna_tipo_motiv_acolhimento('dados.indmotivacol') }},
  dados.indvioldir AS VIOLACAO_DIREITO,
  dados.valpontos AS PONTUACAO
  FROM rj-smas.brutos_acolherio_staging.gh_cidadao_pac pac
  LEFT JOIN rj-smas.brutos_acolherio_staging.gh_atend_familia fam ON fam.seqpac = pac.seqpac
  LEFT JOIN rj-smas.brutos_acolherio_staging.gh_pacientes_sm sm ON sm.seqpacsm = pac.seqpac
  LEFT JOIN rj-smas.brutos_acolherio_staging.gh_pac_dados dados ON dados.seqpac = pac.seqpac
  LEFT JOIN rj-smas.brutos_acolherio_staging.gh_origens ori ON ori.codorigem = sm.codorigem
)

SELECT * FROM  usuario_cadastrado
