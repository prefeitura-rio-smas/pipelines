{{ config(materialized='table') }}

with profissionais as (
    select * from {{ ref('dim_profissionais') }}
),

operadores_unidades as (
    select * from {{ ref('raw_operadores_unidades') }}
),

unidades as (
    select * from {{ ref('dim_unidades') }}
),

joined as (
    select
        {{ dbt_utils.generate_surrogate_key(['prof.id_profissional', 'unid.id_unidade']) }} as id_profissional_unidade_sk,

        -- Profissional (da dim)
        prof.id_profissional_sk,
        prof.id_profissional,
        prof.id_login,
        prof.nome as nome_profissional,
        prof.cpf,
        prof.email_profissional,
        prof.email_operador,
        prof.telefone,
        prof.matricula,
        prof.flag_ativo,
        prof.login_operador,
        prof.qtde_cbos,
        prof.codigos_cbo,
        prof.descricoes_cbo,
        prof.flag_multi_cbo,
        prof.data_cadastro_conta,
        prof.data_ultimo_acesso,
        prof.dias_ultimo_acesso,

        -- Perfil de acesso e status da conta
        prof.nivel_conta,
        prof.perfil_acesso,
        prof.status_conta_codigo,
        prof.status_conta,

        -- Unidades de atuação (da dim)
        prof.unidades_atuacao,
        prof.qtde_unidades,

        -- Unidade (da dim)
        unid.id_unidade_sk,
        unid.id_unidade,
        unid.nome_unidade,
        unid.cas as territorio,
        unid.nome_tipo as tipo_unidade,
        unid.classe as classe_unidade

    from profissionais prof
    left join operadores_unidades op_unid
        on prof.id_login = op_unid.id_login
    left join unidades unid
        on op_unid.id_unidade = unid.id_unidade
)

select * from joined
