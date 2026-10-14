with atendimentos_familias as (
    select * from {{ ref('raw_atendimentos_familias') }}
),

atendimentos_usuarios as (
    select * from {{ ref('raw_atendimentos_usuarios') }}
),

tipos_atendimento as (
    select * from {{ ref('raw_tipos_atendimento') }}
),

-- Mantém a origem para distinguir números de atendimento iguais entre módulos.
uniao_atendimentos_base as (
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
        flag_cancelado,
        id_profissional_compartilhado,
        id_login_cadastro
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
        flag_cancelado,
        id_profissional_compartilhado,
        id_login_cadastro
    from atendimentos_usuarios
),

-- Uma linha por atendimento e profissional, sem repetir IDs compartilhados.
uniao_atendimentos as (
    select
        a.id_atendimento_modulo,
        a.id_atendimento,
        a.id_unidade,
        a.id_usuario,
        id_profissional_normalizado as id_profissional,
        a.id_tipo_atendimento,
        a.data_atendimento,
        a.hora_atendimento,
        a.origem_modulo,
        a.flag_cancelado,
        a.id_login_cadastro
    from uniao_atendimentos_base as a
    cross join unnest({{ ids_profissionais_atendimento(
        'a.id_profissional', 'a.id_profissional_compartilhado'
    ) }}) as id_profissional_normalizado
),

operadores as (
    select * from {{ ref('raw_operadores') }}
),

final as (
    select
        -- A chave inclui o módulo e o profissional para preservar o grão do fato.
        {{ dbt_utils.generate_surrogate_key(['u.id_atendimento_modulo', 'u.id_profissional']) }} as id_atendimento_sk,
        u.id_atendimento_modulo,
        dim_u.id_usuario_sk,
        dim_p.id_profissional_sk,
        dim_un.id_unidade_sk,

        -- Identificadores da origem.
        u.id_usuario,
        u.id_profissional,
        u.id_unidade,
        u.id_atendimento,
        u.id_tipo_atendimento,

        ta.tipo_atendimento_descricao,

        u.data_atendimento,
        u.hora_atendimento,
        u.origem_modulo,
        u.flag_cancelado

    from uniao_atendimentos as u
    left join {{ ref('dim_usuarios') }} as dim_u on u.id_usuario = dim_u.id_usuario
    left join {{ ref('dim_profissionais') }} as dim_p on u.id_profissional = dim_p.id_profissional
    left join {{ ref('dim_unidades') }} as dim_un on u.id_unidade = dim_un.id_unidade
    left join tipos_atendimento as ta on u.id_tipo_atendimento = ta.id_tipo_atendimento
    left join operadores as s on u.id_login_cadastro = s.id_login
    where s.nome_operador is null or s.nome_operador not like '%TESTE%'
)

select * from final
