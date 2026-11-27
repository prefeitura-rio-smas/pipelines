{{ config(enabled=target.name in ["dev", "ci"]) }}

with operadores as (
    select * from {{ ref('raw_operadores') }}
),

unidades_operadores as (
    select * from {{ ref('raw_operadores_unidades') }}
),

unidades as (
    select * from {{ ref('int_unidades') }}
),

unidades_final as (
    select
        ua.id_login,
        ua.id_unidade,
        u.nome_unidade,
        u.cas,
        u.esfera,
        u.email_unidade,
        u.email_filtro,
        u.flag_unidade_ativa,
        u.id_tipo_unidade,
        u.nome_tipo,
        u.classe,
        u.tipo_unidade,
        u.flag_eixo_adulto,
        u.flag_eixo_familia,
        u.flag_eixo_idoso
    from unidades_operadores as ua
    left join unidades as u
        on ua.id_unidade = u.id_unidade
)

select
    ope.id_login as login_operadores,
    ope.id_login,
    ope.nome_operador,
    ope.email,
    ope.login as cpf,
    ope.nivel_conta,

    case
        when ope.nivel_conta = 8 and ope.tipo_acesso_codigo = 'D' then 'Acesso Personalizado (Diretor)'
        when ope.nivel_conta = 8 and ope.tipo_acesso_codigo = 'M' then 'Acesso Personalizado (Master)'
        else {{ map_coluna_perfil_acesso('ope.nivel_conta') }}
    end as perfil_acesso,

    ope.tipo_acesso_codigo,
    ope.status_conta_codigo,
    ope.data_cadastro,
    ope.data_ultimo_acesso,

    -- Dias desde o último acesso
    date_diff(current_date(), ope.data_ultimo_acesso, day) as dias_ultimo_acesso,

    uf.* except (id_login)

from operadores as ope
left join unidades_final as uf
    on ope.id_login = uf.id_login
