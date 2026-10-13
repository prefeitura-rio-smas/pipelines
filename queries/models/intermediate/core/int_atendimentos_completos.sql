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
    select
        *,
        safe_cast(trim(cast(id_profissional as string)) as int64) as profissional_normalizado
    from atendimentos_base
),

atendimentos_explodidos as (
    select
        p.id_atendimento_modulo,
        p.id_atendimento,
        p.id_unidade,
        p.id_usuario,
        p.id_familia,
        p.id_tipo_atendimento,
        p.data_atendimento,
        p.data_cadastro_atendimento,
        p.data_saida,
        p.hora_atendimento,
        p.origem_modulo,
        p.flag_cancelado,
        p.id_login_cadastro,
        profissional_normalizado as id_profissional,
        true as flag_profissional_principal
    from profissionais_por_atendimento as p

    union all

    select
        p.id_atendimento_modulo,
        p.id_atendimento,
        p.id_unidade,
        p.id_usuario,
        p.id_familia,
        p.id_tipo_atendimento,
        p.data_atendimento,
        p.data_cadastro_atendimento,
        p.data_saida,
        p.hora_atendimento,
        p.origem_modulo,
        p.flag_cancelado,
        p.id_login_cadastro,
        profissional_compartilhado as id_profissional,
        false as flag_profissional_principal
    from profissionais_por_atendimento as p,
        unnest(array(
            select distinct safe_cast(trim(codigo) as int64)
            from unnest(split(id_profissional_compartilhado)) as codigo
            where
                safe_cast(trim(codigo) as int64) is not null
                and safe_cast(trim(codigo) as int64) != 0
        )) as profissional_compartilhado
    where profissional_compartilhado is distinct from profissional_normalizado
)

select * from atendimentos_explodidos
