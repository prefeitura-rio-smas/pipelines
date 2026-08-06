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
        safe_cast(trim(pac) as int64) as id_usuario,
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
        e.id_atividade
    from {{ ref('raw_evolucoes_grupo') }} e
    left join {{ ref('raw_atividades_grupo') }} a
        on e.id_atividade = a.id_atividade
    cross join unnest(
        split(replace(replace(e.lista_pacientes, '(', ''), ')', ''), ',')
    ) as pac
    where e.lista_pacientes != '()'
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
        null as id_atividade
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
        null as id_atividade
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
        null as id_atividade
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
        id_atividade
    from grupo
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key([
            'u.origem_modulo',
            'u.id_evolucao',
            'u.id_usuario'
        ]) }} as id_evolucao_sk,
        dim_u.id_usuario_sk,
        dim_p.id_profissional_sk,
        dim_un.id_unidade_sk,
        u.data_evolucao,
        u.descricao_evolucao,
        u.tipo_evolucao,
        u.origem_modulo,
        u.id_familia,
        u.modulo_prontuario,
        u.codigo_abrangencia,
        u.id_paciente_familia,
        u.id_atividade,
        u.id_evolucao_grupo,
        u.id_evolucao
    from uniao u
    left join {{ ref('dim_usuarios') }} dim_u on u.id_usuario = dim_u.id_usuario
    left join {{ ref('dim_profissionais') }} dim_p on u.id_profissional = dim_p.id_profissional
    left join {{ ref('dim_unidades') }} dim_un on u.id_unidade = dim_un.id_unidade
)

select * from final
