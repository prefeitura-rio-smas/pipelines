WITH controle_cas_base AS (
    SELECT
        cpf,
        MAX(
            CASE
                WHEN doc_verificada = 'documentacao_correta' THEN 1
                ELSE 0
            END
        ) AS doc_ok
    FROM {{ source('arcgis_raw', 'controle_cas_raw') }}
    GROUP BY cpf
),

primeira_infancia AS (
    SELECT
        * EXCEPT (verificacao, arquivar_registro),
        NULLIF(verificacao, 'None') AS verificacao,
        NULLIF(arquivar_registro, 'None') AS arquivar_registro
    FROM {{ source('arcgis_raw', 'primeira_infancia_carioca_raw') }}
),

f AS (
    SELECT
        pi.* EXCEPT (verificacao, timestamp_captura),

        CASE
        -- se o usuário JÁ definiu manualmente, respeita
            WHEN pi.verificacao IS NOT NULL THEN pi.verificacao

            -- sugestão automática baseada no CAS
            WHEN cr.doc_ok = 1 THEN 'Verificado'
            ELSE 'Não Verificado'
        END AS verificacao

    FROM primeira_infancia AS pi
    LEFT JOIN controle_cas_base AS cr
        ON pi.cpf = cr.cpf
)

SELECT * FROM f
