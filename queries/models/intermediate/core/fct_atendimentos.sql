-- Mantém o contrato e os filtros dos consumidores atuais do fato.
with atendimentos_explodidos as (
    select * from {{ ref('int_atendimentos_completos') }}
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
