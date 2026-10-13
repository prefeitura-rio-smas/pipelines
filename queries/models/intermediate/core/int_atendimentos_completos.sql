{{ config(materialized='ephemeral') }}

-- Grão: atendimento do módulo × profissional principal ou compartilhado.
with atendimentos_base as (
    select
        id_atendimento_modulo,
        id_atendimento,
        id_unidade,
        id_paciente as id_usuario,
        id_familia,
        id_profissional,
        id_tipo_atendimento,
        data_atendimento,
        data_cadastro_atendimento,
        data_saida,
        hora_atendimento,
        'familia' as origem_modulo,
        flag_cancelado,
        id_profissional_compartilhado,
        id_login_cadastro
    from {{ ref('raw_atendimentos_familias') }}

    union all

    select
        id_atendimento_modulo,
        id_atendimento,
        id_unidade,
        id_paciente as id_usuario,
        cast(null as int64) as id_familia,
        id_profissional,
        id_tipo_atendimento,
        data_atendimento,
        data_cadastro_atendimento,
        data_saida,
        hora_atendimento,
        'usuario' as origem_modulo,
        flag_cancelado,
        id_profissional_compartilhado,
        id_login_cadastro
    from {{ ref('raw_atendimentos_usuarios') }}
),

profissionais_por_atendimento as (
    select * replace (safe_cast(trim(cast(id_profissional as string)) as int64) as id_profissional)
    from atendimentos_base
)

select
    a.id_atendimento_modulo,
    a.id_atendimento,
    a.id_unidade,
    a.id_usuario,
    a.id_familia,
    a.id_tipo_atendimento,
    a.data_atendimento,
    a.data_cadastro_atendimento,
    a.data_saida,
    a.hora_atendimento,
    a.origem_modulo,
    a.flag_cancelado,
    a.id_login_cadastro,
    p.id_profissional,
    p.flag_profissional_principal
from profissionais_por_atendimento as a,
    unnest(array_concat(
        [struct(a.id_profissional, true as flag_profissional_principal)],
        array(
            select as struct
                id_compartilhado as id_profissional,
                false as flag_profissional_principal
            from
                unnest(array(
                    select distinct safe_cast(trim(codigo) as int64)
                    from unnest(split(a.id_profissional_compartilhado)) as codigo
                    where safe_cast(trim(codigo) as int64) is not null and safe_cast(trim(codigo) as int64) != 0
                )) as id_compartilhado
            where id_compartilhado is distinct from a.id_profissional
        )
    )) as p
