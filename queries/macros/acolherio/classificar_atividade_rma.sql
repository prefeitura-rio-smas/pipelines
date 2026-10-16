{# Classificação já utilizada no RMA CRAS; não define elegibilidade dos participantes. #}
{% macro classificar_atividade_rma(relacao) %}
    {{ relacao }}.id_tipo_atividade in (1, 2, 3, 4) as flag_scfv,
    {{ relacao }}.id_tipo_atividade in (5, 6, 7, 8) as flag_nao_continuado,
    (
        coalesce(trim({{ relacao }}.recorrencia), '') != ''
        and regexp_contains(
            concat(
                coalesce({{ relacao }}.nome_atividade, ''), ' ',
                coalesce({{ relacao }}.nome_tipo_atividade, '')
            ),
            r'(?i)(\bPAIF\b|grupo.*fam[ií]lia|fam[ií]lia.*grupo)'
        )
    ) as flag_grupo_regular_paif
{% endmacro %}
