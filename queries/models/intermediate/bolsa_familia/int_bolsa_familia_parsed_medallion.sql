-- Silver: parsing dos layouts de 34 e 35 colunas da folha do Bolsa Família.
-- noqa: disable=CP02

with source as (
    select
        linha_bruta,
        data_particao
    from {{ ref('raw_bolsa_familia__folha') }}
),

c as (
    select
        data_particao,
        split(replace(linha_bruta, '"', ''), ';') as colunas
    from source
    where
        linha_bruta is not null
        and trim(linha_bruta) != ''
),

layout_version as (
    select
        *,
        array_length(colunas) as qtd_cols
    from c
),

parsed as (
    select
        -- Metadados da ingestão
        data_particao,
        qtd_cols as qtd_colunas_brutas,

        -- Layout de 34 colunas (2025 e anteriores) e layout de 35 colunas
        -- (2026+), com uma coluna adicional antes da renda per capita.
        trim(colunas[safe_offset(0)]) as PROG,
        safe.parse_date('%Y%m%d', concat(trim(colunas[safe_offset(1)]), '01')) as REF_FOLHA,
        trim(colunas[safe_offset(2)]) as UF,
        trim(colunas[safe_offset(3)]) as IBGE,
        trim(colunas[safe_offset(4)]) as COD_FAMILIAR,

        -- Identificação do beneficiário
        trim(colunas[safe_offset(5)]) as CPF,
        trim(colunas[safe_offset(6)]) as NIS,
        trim(colunas[safe_offset(7)]) as NOME,

        -- Pagamento e benefício
        trim(colunas[safe_offset(8)]) as TIPO_PGTO_PREVISTO,
        trim(colunas[safe_offset(9)]) as PACTO,
        safe.parse_date('%Y%m%d', concat(trim(colunas[safe_offset(10)]), '01')) as COMPET_PARCELA,
        trim(colunas[safe_offset(11)]) as TP_BENEF,
        safe_cast(replace(trim(colunas[safe_offset(12)]), ',', '.') as numeric) as VLRBENEF,
        safe_cast(replace(trim(colunas[safe_offset(13)]), ',', '.') as numeric) as VLRTOTAL,

        -- Situação e vigência
        trim(colunas[safe_offset(14)]) as SITBENEFICIO,
        trim(colunas[safe_offset(15)]) as SITBENEFICIARIO,
        trim(colunas[safe_offset(16)]) as SITFAM,
        safe.parse_date('%Y%m%d', trim(colunas[safe_offset(17)])) as INICIO_VIG_BENEF,
        safe.parse_date('%Y%m%d', trim(colunas[safe_offset(18)])) as FIM_VIG_BENEF,

        -- Marcadores sociais e de renda
        trim(colunas[safe_offset(19)]) as MARCA_RF,
        trim(colunas[safe_offset(20)]) as QUILOMBOLA,
        trim(colunas[safe_offset(21)]) as TRAB_ESCRV,
        trim(colunas[safe_offset(22)]) as INDIGENA,
        trim(colunas[safe_offset(23)]) as CATADOR_RECIC,
        trim(colunas[safe_offset(24)]) as TRABALHO_INF,

        -- Colunas 25+: offset +1 no layout de 35 colunas.
        safe_cast(replace(trim(colunas[safe_offset(case when qtd_cols = 35 then 26 else 25 end)]), ',', '.') as numeric) as RENDA_PER_CAPITA,
        safe_cast(replace(trim(colunas[safe_offset(case when qtd_cols = 35 then 27 else 26 end)]), ',', '.') as numeric) as RENDA_COM_PBF,
        safe_cast(trim(colunas[safe_offset(case when qtd_cols = 35 then 28 else 27 end)]) as integer) as QTD_PESSOAS,

        -- Dados cadastrais e de contato
        safe.parse_date('%Y%m%d', trim(colunas[safe_offset(case when qtd_cols = 35 then 29 else 28 end)])) as DT_ATU_CADASTRAL,
        trim(colunas[safe_offset(case when qtd_cols = 35 then 30 else 29 end)]) as ENDERECO,
        trim(colunas[safe_offset(case when qtd_cols = 35 then 31 else 30 end)]) as BAIRRO,
        trim(colunas[safe_offset(case when qtd_cols = 35 then 32 else 31 end)]) as CEP,
        trim(colunas[safe_offset(case when qtd_cols = 35 then 33 else 32 end)]) as TELEFONE1,
        trim(colunas[safe_offset(case when qtd_cols = 35 then 34 else 33 end)]) as TELEFONE2
    from layout_version
)

select *
from parsed
