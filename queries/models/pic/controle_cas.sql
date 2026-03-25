WITH controle_cas_base AS (

    SELECT
      * EXCEPT(data_entrega_text, local_entrega, resp_retirada, cartao_entregue),
      NULLIF(data_entrega_text, 'None') as data_entrega_text,
      NULLIF(local_entrega, 'None') as local_entrega,
      NULLIF(resp_retirada, 'None') as resp_retirada,
      CASE
        WHEN NULLIF(data_entrega_text, 'None') IS NOT NULL THEN 'CARTAO ENTREGUE'
      END AS cartao_entregue
    FROM {{ source('arcgis_raw', 'controle_cas_raw') }}

),

-- Dados temporais: limitados à respectiva partição
primeira_infancia_temporal AS (
    SELECT
      cpf,
      SAFE.PARSE_DATE('%d/%m/%Y', data_particao) as data_particao,
      objectid as cod_atend,
      NULLIF(data_entrega, 'None') as data_entrega,
      NULLIF(responsavel_retirada, 'None') as responsavel_retirada,
      NULLIF(arquivar_registro, 'None') as arquivar_registro,
      CASE
        WHEN NULLIF(local_entrega_cras, 'None') IS NULL OR local_entrega_cras = '' THEN local_entrega_outros
        ELSE local_entrega_cras
      END AS local_entrega_cartao
    FROM {{ source('arcgis_raw', 'primeira_infancia_carioca_raw') }}
    WHERE NULLIF(arquivar_registro, 'None') IS NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY cpf, data_particao ORDER BY last_edited_date DESC) = 1
),

-- Dados atemporais: primeira entrega válida (propaga para partições futuras, não para passadas)
primeira_infancia_atemporal AS (
    SELECT
      cpf,
      MIN(CASE WHEN NULLIF(data_entrega, 'None') IS NOT NULL THEN data_particao END) AS data_particao_retirada,
      ARRAY_AGG(
        STRUCT(
          pi_t.data_entrega,
          pi_t.responsavel_retirada,
          pi_t.local_entrega_cartao
        )
        ORDER BY data_particao
        LIMIT 1
      )[OFFSET(0)].*
    FROM (
      SELECT
        cpf,
        SAFE.PARSE_DATE('%d/%m/%Y', data_particao) as data_particao,
        NULLIF(data_entrega, 'None') as data_entrega,
        NULLIF(responsavel_retirada, 'None') as responsavel_retirada,
        CASE
          WHEN NULLIF(local_entrega_cras, 'None') IS NULL OR local_entrega_cras = '' THEN local_entrega_outros
          ELSE local_entrega_cras
        END AS local_entrega_cartao
      FROM {{ source('arcgis_raw', 'primeira_infancia_carioca_raw') }}
      WHERE NULLIF(arquivar_registro, 'None') IS NULL
    ) pi_t
    GROUP BY cpf
),

cc AS (
SELECT
  cc.objectid,
  cc.cpf,
  cc.nome_rf,
  cc.data_nascimento,
  cc.nome_mae,
  cc.cas,
  cc.cras,
  cc.bairro,
  cc.unidade_territorial,
  cc.endereco,
  cc.complemento_ref,
  cc.cep,
  cc.telefone,
  cc.tipo_evento,
  cc.local_entrega_previsto,
  cc.data_entrega_prevista,
  cc.data_entrega,
  -- Propaga local_entrega priorizando a partição atual
  COALESCE(
    pi_t.local_entrega_cartao,
    CASE 
      WHEN pi_a.data_particao_retirada <= SAFE.PARSE_DATE('%d/%m/%Y', cc.data_particao) 
      THEN pi_a.local_entrega_cartao 
    END
  ) AS local_entrega,
  cc.envelope,
  cc.num_cartao_vr,
  cc.nome_cartao_vr,
  -- Propaga cartao_entregue priorizando a partição atual
  CASE 
    WHEN pi_t.data_entrega IS NOT NULL OR pi_a.data_particao_retirada <= SAFE.PARSE_DATE('%d/%m/%Y', cc.data_particao) 
    THEN 'CARTAO ENTREGUE' 
  END AS cartao_entregue,
  cc.doc_verificada,
  cc.resp_verificacao,
  --cc.created_user,
  --cc.created_date,
  --cc.last_edited_user,
  --cc.last_edited_date,
  cc.data_entrega_prevista_2,
  cc.cpf_resp_verific,
  cc.obs,
  -- Propaga data_entrega_text priorizando a partição atual
  COALESCE(
    FORMAT_TIMESTAMP('%d/%m/%Y', TIMESTAMP_MILLIS(CAST(pi_t.data_entrega AS INT64))),
    CASE 
      WHEN pi_a.data_particao_retirada <= SAFE.PARSE_DATE('%d/%m/%Y', cc.data_particao) 
      THEN FORMAT_TIMESTAMP('%d/%m/%Y', TIMESTAMP_MILLIS(CAST(pi_a.data_entrega AS INT64)))
    END
  ) AS data_entrega_text,
  cc.cras_3,
  -- Propaga resp_retirada priorizando a partição atual
  COALESCE(
    pi_t.responsavel_retirada,
    CASE 
      WHEN pi_a.data_particao_retirada <= SAFE.PARSE_DATE('%d/%m/%Y', cc.data_particao) 
      THEN pi_a.responsavel_retirada 
    END
  ) AS resp_retirada,
  cc.telefone_formatado,
  cc.categoria_justificativa,
  FORMAT_DATE('%d/%m/%Y', pi_a.data_particao_retirada) as data_particao_retirada

FROM controle_cas_base cc
LEFT JOIN primeira_infancia_temporal pi_t
  ON cc.cpf = pi_t.cpf AND SAFE.PARSE_DATE('%d/%m/%Y', cc.data_particao) = pi_t.data_particao
LEFT JOIN primeira_infancia_atemporal pi_a
  ON cc.cpf = pi_a.cpf
)

SELECT * FROM cc