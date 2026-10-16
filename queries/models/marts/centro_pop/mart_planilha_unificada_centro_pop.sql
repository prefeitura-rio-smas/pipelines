{{ config(
    tags = ['daily'],
    alias = var('centro_pop_mart_alias', 'mart_planilha_unificada_centro_pop')
) }}

-- Contrato legado preservado; a transformação também serve à mart unificada.
select * from {{ ref('int_planilha_unificada_centro_pop') }}
