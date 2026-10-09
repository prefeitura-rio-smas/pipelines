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
        p.* except (id_profissional, id_profissional_compartilhado, profissional_normalizado),
        profissional_normalizado as id_profissional
    from profissionais_por_atendimento as p

    union all

    select
        p.* except (id_profissional, id_profissional_compartilhado, profissional_normalizado),
        profissional_compartilhado as id_profissional
    from profissionais_por_atendimento as p,
        unnest(array(
            select distinct safe_cast(trim(codigo) as int64)
            from unnest(split(id_profissional_compartilhado)) as codigo
            where
                safe_cast(trim(codigo) as int64) is not null
                and safe_cast(trim(codigo) as int64) != 0
        )) as profissional_compartilhado
    where profissional_compartilhado is distinct from profissional_normalizado
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['a.id_atendimento_modulo', 'a.id_profissional']) }} as id_atendimento_sk,
        a.id_atendimento_modulo,
        case
            when u.id_paciente is not null
                then {{ dbt_utils.generate_surrogate_key(['a.id_usuario']) }}
        end as id_usuario_sk,
        case
            when p.id_profissional is not null
                then {{ dbt_utils.generate_surrogate_key(['a.id_profissional']) }}
        end as id_profissional_sk,
        case
            when un.id_unidade is not null
                then {{ dbt_utils.generate_surrogate_key(['a.id_unidade']) }}
        end as id_unidade_sk,
        a.id_usuario,
        a.id_familia,
        a.id_profissional,
        a.id_unidade,
        a.id_atendimento,
        a.id_tipo_atendimento,
        t.tipo_atendimento_descricao,
        a.data_atendimento,
        a.data_cadastro_atendimento,
        a.data_saida,
        a.hora_atendimento,
        a.origem_modulo,
        a.flag_cancelado,
        a.id_login_cadastro
    from atendimentos_explodidos as a
    -- As chaves das dimensões são hashes dos IDs naturais. Reproduzir a
    -- mesma regra aqui evita reconstruir dimensões sem atributos usados
    -- pelo fato, inclusive a planilha externa de e-mails das unidades.
    left join {{ ref('raw_usuarios') }} as u
        on a.id_usuario = u.id_paciente and u.nome not like '%TESTE%'
    left join {{ ref('raw_profissionais') }} as p
        on a.id_profissional = p.id_profissional and upper(p.nome) not like '%TESTE%'
    left join {{ ref('raw_unidades') }} as un
        on a.id_unidade = un.id_unidade and un.nome_unidade not like '%TESTE%'
    left join {{ ref('raw_tipos_atendimento') }} as t on a.id_tipo_atendimento = t.id_tipo_atendimento
    left join {{ ref('raw_operadores') }} as o on a.id_login_cadastro = o.id_login
    where o.nome_operador is null or o.nome_operador not like '%TESTE%'
)

select * from final
