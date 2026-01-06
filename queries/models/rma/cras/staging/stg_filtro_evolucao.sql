-- Tabela responsável por tratar os dados brutos da tabela gh_evolupac
{{ config(materialized='table') }}

WITH filtro_formulario_extracao_aba AS (
    SELECT 
        sequs,
        seqpac,
        codcateg,
        dtevopac,
        seqlogin,
        codabapac,
        dscevopac,
        dtcancpac,
        seqevopac,
        REGEXP_EXTRACT(dscevopac, r"<h3>(.*?)</h3>") AS aba
    FROM rj-smas.brutos_acolherio_staging.gh_evoluadm
    WHERE indtpevopac = 'F'
),

limpo AS (
  SELECT
    dscevopac,
    sequs,
    seqpac,
    aba,
    codabapac,
    dtevopac,
    REGEXP_REPLACE(dscevopac, '<[^>]+>', ';') AS texto_sem_tags
  FROM filtro_formulario_extracao_aba
  WHERE aba = 'CRAS - Ficha de Atendimento Individualizado'
),

retirar_repetidos_caracteres AS (
  SELECT
    dscevopac,
    sequs,
    seqpac,
    aba,
    codabapac,
    dtevopac,
    REGEXP_REPLACE(texto_sem_tags, ';+', ';') AS texto
  FROM limpo
),

encaminhamentos_limpos AS (
SELECT
    dscevopac,
    sequs,
    seqpac,
    aba,
    codabapac,
    dtevopac,
  -- ATIVIDADES SMAS
  REGEXP_EXTRACT(
    texto,
    r'Encaminhamentos - Atividades SMAS:\s*;([^;]+?)(?:;Encaminhamentos|;Outros|$)'
  ) AS atividades_smas,

  -- BENEFÍCIOS
  REGEXP_EXTRACT(
    texto,
    r'Encaminhamentos - Benefícios:\s*;([^;]+?)(?:;Encaminhamentos|;Outros|$)'
  ) AS beneficios,

  -- ÓRGÃOS
  REGEXP_EXTRACT(
    texto,
    r'Encaminhamentos Órgãos:\s*;([^;]+?)(?:;Encaminhamentos|;Outros|$)'
  ) AS orgaos

FROM retirar_repetidos_caracteres
)

SELECT * FROM encaminhamentos_limpos