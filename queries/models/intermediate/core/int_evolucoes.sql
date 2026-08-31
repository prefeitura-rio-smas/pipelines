with adm as (
    select
        *,
        'administrativa' as origem_modulo
    from {{ ref('raw_evolucoes_administrativas') }}
),

fam as (
    select
        *,
        'familia' as origem_modulo
    from {{ ref('raw_evolucoes_familias') }}
),

usu as (
    select
        *,
        'usuario' as origem_modulo
    from {{ ref('raw_evolucoes_usuarios') }}
),

grupo as (
    select
        a.id_unidade,
        e.id_profissional,
        e.data_evolucao,
        e.descricao_evolucao,
        e.tipo_evolucao,
        'grupo' as origem_modulo,
        null as id_familia,
        null as modulo_prontuario,
        e.codigo_abrangencia,
        null as id_paciente_familia,
        e.id_evolucao_grupo,
        e.id_evolucao,
        e.id_atividade,
        cast(null as datetime) as data_cancelamento,
        safe_cast(trim(pac) as int64) as id_usuario
    from {{ ref('raw_evolucoes_grupo') }} as e
    left join {{ ref('raw_atividades_grupo') }} as a
        on e.id_atividade = a.id_atividade
    cross join
        unnest(
            split(replace(replace(e.lista_pacientes, '(', ''), ')', ''), ',')
        ) as pac
    where
        e.lista_pacientes != '()'
        and e.lista_pacientes is not null
        and e.lista_pacientes != ''
        and trim(pac) != ''
        and e.data_cancelamento is null
),

uniao as (
    select
        id_paciente as id_usuario,
        id_unidade,
        id_login as id_profissional,
        data_evolucao,
        descricao_evolucao,
        tipo_evolucao,
        origem_modulo,
        null as id_familia,
        null as modulo_prontuario,
        null as codigo_abrangencia,
        null as id_paciente_familia,
        null as id_evolucao_grupo,
        id_evolucao,
        null as id_atividade,
        cast(data_cancelamento as datetime) as data_cancelamento
    from adm
    union all
    select
        null as id_usuario,
        id_unidade,
        id_profissional,
        data_evolucao,
        descricao_evolucao,
        tipo_evolucao,
        origem_modulo,
        id_familia,
        modulo_prontuario,
        modulo_prontuario as codigo_abrangencia,
        id_paciente as id_paciente_familia,
        null as id_evolucao_grupo,
        id_evolucao,
        null as id_atividade,
        cast(data_cancelamento as datetime) as data_cancelamento
    from fam
    union all
    select
        id_usuario,
        id_unidade,
        id_profissional,
        data_evolucao,
        descricao_evolucao,
        tipo_evolucao,
        origem_modulo,
        null as id_familia,
        null as modulo_prontuario,
        codigo_abrangencia,
        null as id_paciente_familia,
        null as id_evolucao_grupo,
        id_evolucao,
        null as id_atividade,
        cast(data_cancelamento as datetime) as data_cancelamento
    from usu
    union all
    select
        id_usuario,
        id_unidade,
        id_profissional,
        data_evolucao,
        descricao_evolucao,
        tipo_evolucao,
        origem_modulo,
        null as id_familia,
        null as modulo_prontuario,
        codigo_abrangencia,
        null as id_paciente_familia,
        id_evolucao_grupo,
        id_evolucao,
        id_atividade,
        cast(data_cancelamento as datetime) as data_cancelamento
    from grupo
)

select * from uniao
