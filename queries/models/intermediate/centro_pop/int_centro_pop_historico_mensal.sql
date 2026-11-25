{{ config(enabled=target.name != 'prod', materialized='ephemeral', tags=['centro_pop_mensal', 'planilha_unificada']) }}
-- A reconstrução usa os dados disponíveis a cada execução.
select
    {{ colunas_resumo_mensal_centro_pop('m') }},
    m.atendimentos,
    m.evolucoes,
    m.ocorrencias,
    m.procedimentos_mes,
    m.encaminhamentos_mes,
    m.data_ultimo_atend_tecnico_informada
from {{ ref('int_centro_pop_mensal') }} as m
