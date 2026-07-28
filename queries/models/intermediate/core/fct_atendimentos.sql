with atendimentos_familias as (
    select * from {{ ref('raw_atendimentos_familias') }}
),

atendimentos_usuarios as (
    select * from {{ ref('raw_atendimentos_usuarios') }}
),

tipos_atendimento as (
    select * from {{ ref('raw_tipos_atendimento') }}
),

-- 1. União das tabelas RAW mantendo a sua estrutura original
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
        dsclstprof,
        seqlogincad
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
        dsclstprof,
        seqlogincad
    from atendimentos_usuarios
),

-- 2. Explosão defensiva para incluir profissionais compartilhados
uniao_atendimentos as (
    -- Profissional principal
    select 
        id_atendimento_modulo,
        id_atendimento,
        id_unidade,
        id_usuario,
        safe_cast(trim(cast(id_profissional as string)) as int64) as id_profissional,
        id_tipo_atendimento,
        data_atendimento,
        hora_atendimento,
        origem_modulo,
        flag_cancelado,
        seqlogincad
    from uniao_atendimentos_base

    union all

    -- Profissionais secundários explodidos da lista
    select 
        id_atendimento_modulo,
        id_atendimento,
        id_unidade,
        id_usuario,
        safe_cast(trim(regexp_replace(prof_id, r'^0+', '')) as int64) as id_profissional,
        id_tipo_atendimento,
        data_atendimento,
        hora_atendimento,
        origem_modulo,
        flag_cancelado,
        seqlogincad
    from uniao_atendimentos_base,
    unnest(split(dsclstprof)) as prof_id
    where prof_id is not null 
      and trim(prof_id) != ''
),

operadores as (
    select * from {{ ref('raw_operadores') }}
),

final as (
    select
        -- Surrogate Key garantindo unicidade mesmo com profissionais explodidos
        {{ dbt_utils.generate_surrogate_key(['u.id_atendimento_modulo', 'u.id_profissional']) }} as id_atendimento_sk,
        u.id_atendimento_modulo,
        dim_u.id_usuario_sk,
        dim_p.id_profissional_sk,
        dim_un.id_unidade_sk,
        
        -- IDs originais (Mantidos intactos)
        u.id_usuario,
        u.id_profissional,
        u.id_unidade,
        u.id_atendimento,
        u.id_tipo_atendimento,
        
        -- Atributos enriquecidos das RAWs existentes
        ta.dsctpatend as tipo_atendimento_descricao,
        
        u.data_atendimento,
        
        -- Formatação de hora tratada (HH:MM)
        format('%02d:%02d', 
            div(safe_cast(u.hora_atendimento as int64), 100), 
            mod(safe_cast(u.hora_atendimento as int64), 100)
        ) as hora_atendimento,
        
        u.origem_modulo,
        u.flag_cancelado,

        -------------------------------------------------------------
        
        -------------------------------------------------------------
        dim_p.nome_profissional as profissional,
        dim_p.cbo_descricao as profissional_cbo,
        dim_p.cbo_descricao as profissional_cbo_original,
        
        dim_u.idade,
        
        dim_un.nome_unidade as unidade_atendimento,
        dim_un.tipo_unidade,
        dim_un.email_unidade,
        dim_un.email_cas,
        
        ta.dsctpatend as nome_atendimento_original,
        u.data_atendimento as data_de_atendimento,
        u.data_atendimento as data_cadastro_atendimento,
        
        format('%02d:%02d', 
            div(safe_cast(u.hora_atendimento as int64), 100), 
            mod(safe_cast(u.hora_atendimento as int64), 100)
        ) as hora_de_atendimento

    from uniao_atendimentos u
    left join {{ ref('dim_usuarios') }} dim_u on u.id_usuario = dim_u.id_usuario
    left join {{ ref('dim_profissionais') }} dim_p on u.id_profissional = dim_p.id_profissional
    left join {{ ref('dim_unidades') }} dim_un on u.id_unidade = dim_un.id_unidade
    left join tipos_atendimento ta on u.id_tipo_atendimento = ta.seqtpatend
    left join operadores s on s.id_operador = u.seqlogincad
    where 
        (s.nome_operador is null or s.nome_operador not like '%TESTE%')
)

select * from final