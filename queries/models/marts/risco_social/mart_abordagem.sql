-- Mart de Abordagem Social (situação de rua): 1 linha por abordagem (globalid)
-- registrada no Feature Service, com o cluster de duplicidade da pessoa, o
-- e-mail institucional da unidade (filtro do BI) e o objectid da ficha de origem.
--
-- CONTRATO EXTERNO (transição BI): materializa em `rj-smas-dev.dashboard_arcgis.abordagem`
-- com o MESMO nome de tabela e as MESMAS colunas da antiga tabela do
-- `old_architecture/dashboard_arcgis` — o Looker lê essa relação direto.
-- Não renomear/remover coluna sem repontar o dashboard (histórico: ~11 mil
-- queries/semana no dataset). O `database`/`schema` condicionais ficam no
-- mart_abordagem.yml e servem só para preservar esse contrato em prod; em
-- dev/CI o modelo escreve no dataset do target. Quando o BI for repontado,
-- remover as duas linhas do yml e o modelo volta ao schema padrão de marts.

{{ config(
    materialized = 'table',
    alias = 'abordagem',
    tags = ['abordagem']
) }}

SELECT
    r.*,  -- noqa: AM04 (contrato: as 148 colunas da raw de abordagem)
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
