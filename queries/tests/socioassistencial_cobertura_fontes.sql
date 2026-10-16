-- A cobertura é independente de elegibilidade e do relacionamento cadastral.
with esperados as (
    select
        'ATENDIMENTO' as tipo_registro,
        id_atendimento_modulo as id_registro_origem
    from {{ ref('raw_atendimentos_usuarios') }}
    union all
    select
        'ATENDIMENTO' as tipo_registro,
        id_atendimento_modulo
    from {{ ref('raw_atendimentos_familias') }}
    union all
    select
        'ABORDAGEM' as tipo_registro,
        globalid
    from {{ ref('abordagem_repeat') }}
    union all
    select
        'VINCULO_SERVICO' as tipo_registro,
        id_registro_origem
    from {{ ref('int_socioassistencial_vinculos_servicos') }}
    union all
    select
        'ATIVIDADE_COLETIVA' as tipo_registro,
        id_registro_origem
    from {{ ref('int_socioassistencial_atividades') }}
    union all
    select
        'FORMULARIO' as tipo_registro,
        id_registro_origem
    from {{ ref('int_socioassistencial_formularios') }}
    union all
    select
        'ENCAMINHAMENTO' as tipo_registro,
        id_registro_origem
    from {{ ref('int_socioassistencial_encaminhamentos') }}
),

publicados as (
    select
        tipo_registro,
        id_registro_origem
    from {{ ref('mart_socioassistencial_unificada') }}
)

select
    tipo_registro,
    id_registro_origem
from esperados
except distinct
select
    tipo_registro,
    id_registro_origem
from publicados
