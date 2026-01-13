-- Tabela responsável por capturar os dados de evolução e seus encaminhamentos.

with filtro_encaminhamentos_evolucao as (
    select
        aba,
        dscevopac_limpo,
        data_evolucao,
        sequs,
        seqpac,
        seqlogin,
        regexp_extract(
            dscevopac_limpo,
            r'Encaminhamentos - Atividades SMAS:\s*;([^;]+?)(?:;Encaminhamentos|;Outros|$)'
        ) as encaminhamento_atv_smas,
        regexp_extract(
            dscevopac_limpo,
            r'Encaminhamentos - Benefícios:\s*;([^;]+?)(?:;Encaminhamentos|;Outros|$)'
        ) as encaminhamento_beneficios,
        regexp_extract(
            dscevopac_limpo,
            r'Encaminhamentos Órgãos:\s*;([^;]+?)(?:;Encaminhamentos|;Outros|$)'
        ) as encaminhamento_orgaos
    from {{ ref('stg_evolucao') }}
)

select * from filtro_encaminhamentos_evolucao