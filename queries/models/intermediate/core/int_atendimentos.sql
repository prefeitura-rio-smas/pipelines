-- Camada Intermediate: Atendimentos (input da fct_atendimentos)
-- Grão: 1 linha por atendimento e profissional. A surrogate key e os joins de
-- dimensão ficam na fct_atendimentos (mart).
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

-- Normaliza o principal uma vez, antes de combinar os profissionais do atendimento.
atendimentos_normalizados as (
    select * replace (safe_cast(trim(cast(id_profissional as string)) as int64) as id_profissional)
    from uniao_atendimentos_base
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
    from atendimentos_normalizados as a
    cross join unnest(array_concat(
        -- Preserva o principal, inclusive quando desconhecido, como primeiro elemento.
        [a.id_profissional],
        array(
            select distinct id_compartilhado
            from (
                select safe_cast(trim(valor) as int64) as id_compartilhado
                from unnest(split(a.id_profissional_compartilhado, ',')) as valor
            ) as compartilhados
            where
                id_compartilhado is not null
                and id_compartilhado != 0
                and id_compartilhado is distinct from a.id_profissional
        )
    )) as id_profissional_normalizado
),

operadores as (
    select * from {{ ref('raw_operadores') }}
),

final as (
    select
        u.id_atendimento_modulo,

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
    left join tipos_atendimento as ta on u.id_tipo_atendimento = ta.id_tipo_atendimento
    left join operadores as s on u.id_login_cadastro = s.id_login
    where s.nome_operador is null or s.nome_operador not like '%TESTE%'
)

select * from final
