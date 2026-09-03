{% macro extrair_encaminhamentos(source_relation, id_cols, col_html='descricao_evolucao') %}
-- Extrai os campos de encaminhamento (SMAS, Benefícios, Órgãos) do HTML das
-- evoluções. Generalista: serve ao RMA CRAS, ao RMA CREAS e ao pipeline core
-- (o escopo por módulo/tipo/aba fica no modelo chamador).
-- source_relation aceita ref() ou nome de CTE.
(
    select
        {{ id_cols | join(', ') }},
        regexp_extract(
            descricao_limpa,
            r'Encaminhamentos - (?:Atividades )?SMAS:\s*;?([^;]+?)(?:;Encaminhamentos|;Outros|$)'
        ) as encaminhamento_smas,
        regexp_extract(
            descricao_limpa,
            r'Encaminhamentos - Benefícios:\s*;?([^;]+?)(?:;Encaminhamentos|;Outros|$)'
        ) as encaminhamento_beneficios,
        regexp_extract(
            descricao_limpa,
            r'Encaminhamentos Órgãos:\s*;?([^;]+?)(?:;Encaminhamentos|;Outros|$)'
        ) as encaminhamento_orgaos
    from (
        select
            {{ id_cols | join(', ') }},
            regexp_replace(
                regexp_replace({{ col_html }}, '<[^>]+>', ';'),
                ';+',
                ';'
            ) as descricao_limpa
        from {{ source_relation }}
    )
)
{% endmacro %}
