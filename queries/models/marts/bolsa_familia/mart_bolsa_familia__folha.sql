-- Gold: folha consolidada do Bolsa Família.
-- O alias temporário evita colisão com a tabela física legada `folha`.
-- depends_on: {{ ref('raw_bolsa_familia__folha') }}
-- noqa: disable=CP02

{{ config(
    alias='folha_medallion',
    materialized='incremental',
    partition_by={
      "field": "data_particao",
      "data_type": "date",
      "granularity": "day"
    },
    incremental_strategy='insert_overwrite'
) }}

with parsed as (
    select *
    from {{ ref('int_bolsa_familia_parsed_medallion') }} as parsed_source
    {% if is_incremental() %}
        -- Processa apenas as partições carregadas no staging.
        where parsed_source.data_particao in (
            select distinct raw_source.data_particao
            from {{ ref('raw_bolsa_familia__folha') }} as raw_source
        )
    {% endif %}
),

with_family_sum as (
    select
        *,
        sum(VLRBENEF) over (
            partition by data_particao, COD_FAMILIAR
        ) as beneficio_familiar_valor
    from parsed
)

select
    -- Mantém o mesmo contrato de saída da mart legada. O metadado
    -- `qtd_colunas_brutas` permanece disponível na camada intermediate,
    -- mas não é exposto na tabela final para garantir compatibilidade.
    data_particao,
    PROG,
    REF_FOLHA,
    UF,
    IBGE,
    COD_FAMILIAR,
    CPF,
    NIS,
    NOME,
    TIPO_PGTO_PREVISTO,
    PACTO,
    COMPET_PARCELA,
    TP_BENEF,
    VLRBENEF,
    VLRTOTAL,
    SITBENEFICIO,
    SITBENEFICIARIO,
    SITFAM,
    INICIO_VIG_BENEF,
    FIM_VIG_BENEF,
    MARCA_RF,
    QUILOMBOLA,
    TRAB_ESCRV,
    INDIGENA,
    CATADOR_RECIC,
    TRABALHO_INF,
    RENDA_PER_CAPITA,
    RENDA_COM_PBF,
    QTD_PESSOAS,
    DT_ATU_CADASTRAL,
    ENDERECO,
    BAIRRO,
    CEP,
    TELEFONE1,
    TELEFONE2,
    beneficio_familiar_valor,
    case
        when beneficio_familiar_valor <= 374 then 'ate 374'
        when beneficio_familiar_valor <= 600 then '375 a 600'
        when beneficio_familiar_valor <= 750 then '601 a 750'
        when beneficio_familiar_valor <= 1350 then '751 a 1350'
        else 'acima de 1350'
    end as beneficio_familiar_faixa
from with_family_sum
