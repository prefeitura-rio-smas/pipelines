-- Reconstitui o universo esperado diretamente da fonte. O teste não exige
-- presença em atendimento, responsável cadastrado ou família com membros.
with esperados as (
    select distinct
        v.id_familia,
        v.id_servico_assistencial,
        v.id_unidade,
        competencia
    from {{ ref('raw_familias_servicos_assistenciais') }} as v,
        unnest(generate_date_array(
            date_trunc(date(v.data_cadastro), month),
            least(coalesce(date_trunc(date(v.data_cancelamento), month), {{ mes_referencia() }}), {{ mes_referencia() }}, {{ inicio_mes('current_date()') }}),
            interval 1 month
        )) as competencia
    where v.data_cancelamento is null or v.data_cancelamento >= v.data_cadastro
),

publicados as (
    select
        id_familia,
        acompanhamento.id_servico as id_servico_assistencial,
        id_unidade,
        competencia
    from {{ ref('mart_socioassistencial_unificada') }}
    where tipo_registro = 'ACOMPANHAMENTO_MENSAL'
),

ausentes as (
    select
        id_familia,
        id_servico_assistencial,
        id_unidade,
        competencia
    from esperados
    except distinct
    select
        id_familia,
        id_servico_assistencial,
        id_unidade,
        competencia
    from publicados
),

inesperados as (
    select
        id_familia,
        id_servico_assistencial,
        id_unidade,
        competencia
    from publicados
    except distinct
    select
        id_familia,
        id_servico_assistencial,
        id_unidade,
        competencia
    from esperados
)

select
    id_familia,
    id_servico_assistencial,
    id_unidade,
    competencia
from ausentes
union all
select
    id_familia,
    id_servico_assistencial,
    id_unidade,
    competencia
from inesperados
