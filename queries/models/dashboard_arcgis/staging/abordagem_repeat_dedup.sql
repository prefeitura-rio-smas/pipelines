{{ config(materialized = 'ephemeral') }}

-- ==========================================================
-- Dedup via grafo recursivo com 6 regras de matching exato
-- Substitui o modelo anterior baseado em SOUNDEX
-- ==========================================================

WITH RECURSIVE

BaseDados AS (
  SELECT
    globalid as id,
    key_cpf,
    key_nome_usuario as key_nm,
    key_nome_mae      as key_mae,
    key_nome_pai      as key_pai,
    repeat_data_nascimento,
    ano_num_data_abordagem as ano
  FROM {{ ref('abordagem_repeat') }}
),

Matches AS (
  -- REGRA 1: CPF
  SELECT id AS id_origem, MIN(id) OVER (PARTITION BY key_cpf, ano) AS id_destino
  FROM BaseDados WHERE key_cpf NOT IN ('99999999999','00000000000','11111111111') AND LENGTH(key_cpf)=11 QUALIFY id != id_destino

  UNION ALL

  -- REGRA 2: NOME USUARIO + NOME MAE
  SELECT id AS id_origem, MIN(id) OVER (PARTITION BY key_nm, key_mae, ano) AS id_destino
  FROM BaseDados WHERE key_nm!='' AND key_mae!='' QUALIFY id != id_destino

  UNION ALL

  -- REGRA 3: NOME + CPF + DATA NASCIMENTO
  SELECT id AS id_origem, MIN(id) OVER (PARTITION BY key_nm, key_cpf, repeat_data_nascimento, ano) AS id_destino
  FROM BaseDados WHERE key_nm!='' AND LENGTH(key_cpf)=11 AND repeat_data_nascimento IS NOT NULL QUALIFY id != id_destino

  UNION ALL

  -- REGRA 4: NOME MAE + CPF + DATA NASCIMENTO
  SELECT id AS id_origem, MIN(id) OVER (PARTITION BY key_mae, key_cpf, repeat_data_nascimento, ano) AS id_destino
  FROM BaseDados WHERE key_mae!='' AND LENGTH(key_cpf)=11 AND repeat_data_nascimento IS NOT NULL QUALIFY id != id_destino

  UNION ALL

  -- REGRA 5: NOME MAE + NOME PAI + DATA NASCIMENTO
  SELECT id AS id_origem, MIN(id) OVER (PARTITION BY key_mae, key_pai, repeat_data_nascimento, ano) AS id_destino
  FROM BaseDados WHERE key_mae!='' AND key_pai!='' AND repeat_data_nascimento IS NOT NULL QUALIFY id != id_destino

  UNION ALL

  -- REGRA 6: NOME MAE + NOME PAI + CPF
  SELECT id AS id_origem, MIN(id) OVER (PARTITION BY key_mae, key_pai, key_cpf, ano) AS id_destino
  FROM BaseDados WHERE key_mae!='' AND key_pai!='' AND LENGTH(key_cpf)=11 QUALIFY id != id_destino
),

GrafoConexo AS (
  SELECT id_origem, id_destino FROM Matches
  UNION ALL
  SELECT G.id_origem, M.id_destino
  FROM GrafoConexo G JOIN Matches M ON G.id_destino = M.id_origem
),

Mapa_De_Para AS (
  SELECT id_origem AS ID_Cliente, MIN(id_destino) AS ID_Mestre
  FROM GrafoConexo GROUP BY id_origem
)

SELECT
  Orig.id AS globalid,
  COALESCE(Mapa.ID_Mestre, Orig.id) AS cluster_id,
  Mapa.ID_Mestre IS NOT NULL AND Mapa.ID_Mestre != Orig.id AS is_duplicado
FROM BaseDados Orig
LEFT JOIN Mapa_De_Para Mapa ON Orig.id = Mapa.ID_Cliente
