-- Divergências são evidências, não testes de igualdade entre universos diferentes.
-- Paridade estrutural de atendimento/planilha está nos testes singulares.
with dataviz_nova as (
    select
        m.competencia,
        m.nome_unidade,
        count(*) as quantidade
    from {{ ref('mart_socioassistencial_unificada') }} as m,
        unnest(m.atendimento.profissionais) as p
    where
        m.tipo_registro = 'ATENDIMENTO'
        and m.atendimento.flag_elegivel_dataviz
        and p.atendimento_unico_rank = 1
    group by m.competencia, m.nome_unidade
),

dataviz_programada as (
    select
        unidade_atendimento as nome_unidade,
        date_trunc(date(data_de_atendimento), month) as competencia,
        count(*) as quantidade
    from `rj-smas-dev.dashboard_acolherio.dev_atendimentos`
    group by competencia, nome_unidade
),

cras_nova as (
    select
        id_unidade,
        count(distinct if(
            not regexp_contains(atendimento.nome_atendimento_original, '(?i)recepção'), id_registro, null
        )) as c1,
        count(distinct if(
            regexp_contains(atendimento.nome_atendimento_original, '(?i)domiciliar'), id_registro, null
        )) as c6
    from {{ ref('mart_socioassistencial_unificada') }}
    where
        tipo_registro = 'ATENDIMENTO'
        and competencia = {{ mes_referencia() }}
        and {{ nao_cancelado('atendimento.flag_cancelado') }}
        and (atendimento.cadastrante is null or atendimento.cadastrante not like '%TESTE%')
    group by id_unidade
),

comparacao_cras as (
    select
        c.nome_unidade,
        i.indicador,
        i.anterior,
        i.nova
    from {{ ref('mart_rma_cras__indicadores') }} as c
    left join cras_nova as n on c.id_unidade = n.id_unidade,
        unnest([
            struct('CRAS_C1' as indicador, c.total_atendimentos_c1 as anterior, coalesce(n.c1, 0) as nova),
            struct('CRAS_C6' as indicador, c.total_atendimentos_domiciliar_c6 as anterior, coalesce(n.c6, 0) as nova)
        ]) as i
)

select
    'DATAVIZ_PARTICIPACOES' as indicador,
    coalesce(n.competencia, a.competencia) as competencia,
    coalesce(n.nome_unidade, a.nome_unidade) as nome_unidade,
    a.quantidade as anterior,
    n.quantidade as nova,
    coalesce(n.quantidade, 0) - coalesce(a.quantidade, 0) as diferenca
from dataviz_nova as n
full outer join dataviz_programada as a
    on n.competencia = a.competencia and n.nome_unidade = a.nome_unidade
union all
select
    indicador,
    {{ mes_referencia() }} as competencia,
    nome_unidade,
    anterior,
    nova,
    nova - anterior as diferenca
from comparacao_cras
