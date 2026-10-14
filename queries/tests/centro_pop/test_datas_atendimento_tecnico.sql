select
    id_atendimento,
    data_atendimento,
    data_ultimo_atendimento_tecnico_mes,
    data_referencia
from {{ ref('mart_planilha_unificada_centro_pop') }}
where
    data_ultimo_atendimento_tecnico_mes > data_referencia
    or (
        tipo_atendimento = 'Atendimento Técnico'
        and (
            data_ultimo_atendimento_tecnico_mes is null
            or data_ultimo_atendimento_tecnico_mes < data_atendimento
        )
    )
    or (
        id_usuario is null
        and tipo_atendimento = 'Atendimento Técnico'
        and data_ultimo_atendimento_tecnico_mes is distinct from data_atendimento
    )
