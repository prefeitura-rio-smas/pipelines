{{ config(materialized='ephemeral', enabled=target.name != 'prod', tags=['planilha_unificada']) }}
with eventos as (
    select
        modulo,
        id_atendimento,
        id_paciente as id_usuario,
        id_unidade,
        data_atendimento
    from {{ ref('raw_atendimentos_usuarios') }}
    union all
    select
        modulo,
        id_atendimento,
        id_paciente as id_usuario,
        id_unidade,
        data_atendimento
    from {{ ref('raw_atendimentos_familias') }}
)

select
    modulo,
    id_atendimento,
    id_usuario,
    id_unidade,
    if(count(distinct data_atendimento) > 1, error('Datas conflitantes para atendimento'), min(data_atendimento)) as data_atendimento,
    if(count(data_atendimento) > 0, 'atendimento', 'ausente') as origem_data_atendimento
from eventos
group by modulo, id_atendimento, id_usuario, id_unidade
