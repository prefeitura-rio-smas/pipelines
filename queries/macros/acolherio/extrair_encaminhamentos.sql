{% macro extrair_encaminhamentos(source_relation, pass_through_cols, col_html='descricao_evolucao') %}
-- Extrai os campos de encaminhamento (SMAS, Benefícios, Órgãos) do HTML das
-- evoluções. Generalista: serve ao RMA CRAS, ao RMA CREAS e ao pipeline core.
-- pass_through_cols: array com as colunas que devem ser repassadas além das
-- extraídas (obrigatório — select * não funciona por causa do subquery wrapper).
-- source_relation aceita ref() ou nome de CTE.
(
    select
        {{ pass_through_cols | join(', ') }},
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
            {{ pass_through_cols | join(', ') }},
            regexp_replace(
                regexp_replace({{ col_html }}, '<[^>]+>', ';'),
                ';+',
                ';'
            ) as descricao_limpa
        from {{ source_relation }}
    )
)
{% endmacro %}
