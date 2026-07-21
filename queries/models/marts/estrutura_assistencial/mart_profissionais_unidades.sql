{{ config(materialized='table', tags=['daily']) }}

with profissionais as (
    select * from {{ ref('dim_profissionais') }}
),

unidades as (
    select * from {{ ref('dim_unidades') }}
),

filtro_email as (
    select * from {{ ref('raw_sheets_filtro_email_prontuario') }}
),

joined as (
    select
        {{ dbt_utils.generate_surrogate_key(['prof.id_profissional', 'unid.id_unidade']) }} as id_profissional_unidade_sk,

        -- Profissional (da dim)
        prof.id_profissional_sk,
        prof.id_profissional,
        prof.id_login,
        UPPER(prof.nome_cadastral) as nome_profissional,
        UPPER(prof.nome_operador) as nome_operador,
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
        prof.flag_sem_conta,

        -- Unidades de atuacao (da dim)
        UPPER(prof.unidades_atuacao) as unidades_atuacao,
        prof.qtde_unidades,

        -- Unidade (da dim)
        unid.id_unidade_sk,
        unid.id_unidade,
        UPPER(unid.nome_unidade) as nome_unidade,
        unid.cas as territorio,
        unid.nome_tipo as tipo_unidade,
        unid.classe as classe_unidade,

        -- Flag indicando se o profissional tem unidade valida na dim_unidades
        (unid.id_unidade IS NOT NULL) as flag_unidade_valida,

        -- Email da unidade via planilha de filtro
        fe.email as email_unidade

    from profissionais prof
    left join unnest(prof.ids_unidade) as id_unidade
    left join unidades unid
        on unid.id_unidade = id_unidade
    left join filtro_email fe
        on unid.nome_unidade = upper(fe.unidade_atendimento)
)

select * from joined
where not flag_sem_conta
