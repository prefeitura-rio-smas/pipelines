-- Mart de Abordagem Social (situação de rua): 1 linha por abordagem (globalid)
-- registrada no Feature Service, com o cluster de duplicidade da pessoa, o
-- e-mail institucional da unidade (filtro do BI) e o objectid da ficha de origem.
--
-- Destino: `rj-smas.abordagem.abordagem` em prod. O schema vem da configuração
-- de pasta no dbt_project (`marts.abordagem: +schema`), não de hardcode no
-- modelo; em dev/CI o target resolve para `rj-smas-dev.relatorio` e
-- `ci_<PR>__<SHA>` respectivamente.
-- A tabela antiga `rj-smas-dev.dashboard_arcgis.abordagem` deixa de ser
-- atualizada; o repoint do Looker é uma etapa operacional posterior.
--
-- O `r.*` é intencional: contrato das 149 colunas da raw (154 no total com os
-- 5 joins). O noqa abaixo suprime o AM04 (colunas desconhecidas) neste SELECT.

{{ config(
    materialized = 'table',
    alias = 'abordagem',
    tags = ['abordagem']
) }}

SELECT
    r.*,  -- noqa: AM04
    e.cas,
    e.e_mail,
    c.cluster_id,
    c.is_duplicado,
    f.objectid AS ficha_objectid
FROM
    {{ ref('raw_abordagem_repeat') }} AS r
LEFT JOIN
    {{ source('dashboard_arcgis_manuais', 'abordagem_filtro_emails') }} AS e
    ON
        r.repeat_unidade_cas = e.cas
LEFT JOIN
    {{ ref('int_abordagem_repeat_dedup') }} AS c
    ON
        r.globalid = c.globalid
LEFT JOIN
    {{ ref('raw_abordagem_ficha') }} AS f
    ON
        r.parentrowid = CAST(f.objectid AS STRING)
