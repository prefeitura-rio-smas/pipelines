with atendimentos_familias as (
    select * from {{ ref('raw_atendimentos_familias') }}
),

atendimentos_usuarios as (
    select * from {{ ref('raw_atendimentos_usuarios') }}
),

uniao_atendimentos as (
    select
        id_atendimento_modulo,
        id_atendimento,
        id_unidade,
        id_paciente as id_usuario,
        id_profissional,
        id_tipo_atendimento,
        data_atendimento,
        hora_atendimento,
        'familia' as origem_modulo,
        flag_cancelado
    from atendimentos_familias

    union all

    select
        id_atendimento_modulo,
        id_atendimento,
        id_unidade,
        id_paciente as id_usuario,
        id_profissional,
        id_tipo_atendimento,
        data_atendimento,
        hora_atendimento,
        'usuario' as origem_modulo,
        flag_cancelado
    from atendimentos_usuarios
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['u.id_atendimento_modulo']) }} as id_atendimento_sk,
        u.id_atendimento_modulo,
        dim_u.id_usuario_sk,
        dim_p.id_profissional_sk,
        dim_un.id_unidade_sk,
        -- IDs originais para auditoria rápida
        u.id_usuario,
        u.id_profissional,
        u.id_unidade,
        u.id_atendimento,
        u.data_atendimento,
        u.hora_atendimento,
        u.origem_modulo,
        u.flag_cancelado
    from uniao_atendimentos u
    left join {{ ref('dim_usuarios') }} dim_u on u.id_usuario = dim_u.id_usuario
    left join {{ ref('dim_profissionais') }} dim_p on u.id_profissional = dim_p.id_profissional
    left join {{ ref('dim_unidades') }} dim_un on u.id_unidade = dim_un.id_unidade
)

select * from final
