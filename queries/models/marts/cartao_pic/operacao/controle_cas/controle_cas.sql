WITH controle_cas_base AS (

    SELECT
        * EXCEPT (data_entrega_text, local_entrega, resp_retirada, cartao_entregue, data_particao),
        -- Tratamento robusto da data da folha (ArcGIS)
        COALESCE(
            SAFE.PARSE_DATE('%Y-%m-%d', data_particao),
            SAFE.PARSE_DATE('%d/%m/%Y', data_particao)
        ) AS data_particao,
        NULLIF(data_entrega_text, 'None') AS data_entrega_text,
        NULLIF(local_entrega, 'None') AS local_entrega,
        NULLIF(resp_retirada, 'None') AS resp_retirada,
        CASE
            WHEN NULLIF(data_entrega_text, 'None') IS NOT NULL THEN 'CARTAO ENTREGUE'
        END AS cartao_entregue
    FROM {{ source('arcgis_raw', 'controle_cas_raw') }}

),

-- Dados temporais: limitados à respectiva partição
primeira_infancia_temporal AS (
    SELECT
        cpf,
        -- Tratamento robusto da data da folha (Survey)
        objectid AS cod_atend,
        COALESCE(
            SAFE.PARSE_DATE('%Y-%m-%d', data_particao),
            SAFE.PARSE_DATE('%d/%m/%Y', data_particao)
        ) AS data_particao,
        NULLIF(data_entrega, 'None') AS data_entrega,
        NULLIF(responsavel_retirada, 'None') AS responsavel_retirada,
        NULLIF(arquivar_registro, 'None') AS arquivar_registro,
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
        ARRAY_AGG(  -- noqa: AL03
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
            COALESCE(
                SAFE.PARSE_DATE('%Y-%m-%d', data_particao),
                SAFE.PARSE_DATE('%d/%m/%Y', data_particao)
            ) AS data_particao,
            NULLIF(data_entrega, 'None') AS data_entrega,
            NULLIF(responsavel_retirada, 'None') AS responsavel_retirada,
            CASE
                WHEN NULLIF(local_entrega_cras, 'None') IS NULL OR local_entrega_cras = '' THEN local_entrega_outros
                ELSE local_entrega_cras
            END AS local_entrega_cartao
        FROM {{ source('arcgis_raw', 'primeira_infancia_carioca_raw') }}
        WHERE NULLIF(arquivar_registro, 'None') IS NULL
    ) AS pi_t
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
        cc.envelope,
        cc.num_cartao_vr,
        cc.nome_cartao_vr,
        cc.doc_verificada,
        -- Propaga cartao_entregue priorizando a partição atual
        cc.resp_verificacao,
        cc.data_entrega_prevista_2,
        cc.cpf_resp_verific,
        --cc.created_user,
        --cc.created_date,
        --cc.last_edited_user,
        --cc.last_edited_date,
        cc.obs,
        cc.cras_3,
        cc.telefone_formatado,
        -- Propaga data_entrega_text priorizando a partição atual
        cc.categoria_justificativa,
        CAST(pi_a.data_particao_retirada AS STRING) AS data_particao_retirada,
        -- Propaga resp_retirada priorizando a partição atual
        CAST(cc.data_particao AS STRING) AS data_particao,
        COALESCE(
            pi_t.local_entrega_cartao,
            CASE
                WHEN pi_a.data_particao_retirada <= cc.data_particao
                    THEN pi_a.local_entrega_cartao
            END
        ) AS local_entrega,
        CASE
            WHEN pi_t.data_entrega IS NOT NULL OR pi_a.data_particao_retirada <= cc.data_particao
                THEN 'CARTAO ENTREGUE'
        END AS cartao_entregue,
        -- Saída final padronizada como ISO String para o ArcGIS
        COALESCE(
            FORMAT_TIMESTAMP('%d/%m/%Y', TIMESTAMP_MILLIS(CAST(pi_t.data_entrega AS INT64))),
            CASE
                WHEN pi_a.data_particao_retirada <= cc.data_particao
                    THEN FORMAT_TIMESTAMP('%d/%m/%Y', TIMESTAMP_MILLIS(CAST(pi_a.data_entrega AS INT64)))
            END
        ) AS data_entrega_text,
        COALESCE(
            pi_t.responsavel_retirada,
            CASE
                WHEN pi_a.data_particao_retirada <= cc.data_particao
                    THEN pi_a.responsavel_retirada
            END
        ) AS resp_retirada

    FROM controle_cas_base AS cc
    LEFT JOIN primeira_infancia_temporal AS pi_t
        ON cc.cpf = pi_t.cpf AND cc.data_particao = pi_t.data_particao
    LEFT JOIN primeira_infancia_atemporal AS pi_a
        ON cc.cpf = pi_a.cpf
)

SELECT * FROM cc
