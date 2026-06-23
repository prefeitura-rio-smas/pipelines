-- Camada Intermediate: Dimensão Profissionais
-- Grão: 1 linha por profissional
-- Inclui dados do operador (conta de login), CBOs agregados,
-- perfil de acesso e status da conta
with profissionais as (
    select * from {{ ref('raw_profissionais') }}
),

operadores as (
    select * from {{ ref('raw_operadores') }}
),

-- Junta ocupações com CBO antes de agregar para ter descrições
profissionais_ocupacoes_detalhadas as (
    select
        poc.id_profissional,
        poc.codigo_cbo,
        c.descricao
    from {{ ref('raw_profissionais_ocupacoes') }} poc
    left join {{ ref('raw_cbo') }} c on poc.codigo_cbo = c.codigo_cbo
),

ocupacoes as (
    select
        id_profissional,
        count(*) as qtde_cbos,
        string_agg(cast(codigo_cbo as string), ' | ' order by codigo_cbo) as codigos_cbo,
        string_agg(descricao, ' | ' order by codigo_cbo) as descricoes_cbo
    from profissionais_ocupacoes_detalhadas
    group by 1
),

unidades_atuacao as (
    select
        op_unid.id_login,
        string_agg(u.nome_unidade, ' | ' order by u.nome_unidade) as unidades_atuacao,
        count(distinct u.id_unidade) as qtde_unidades
    from {{ ref('raw_operadores_unidades') }} op_unid
    left join {{ ref('raw_unidades') }} u on op_unid.id_unidade = u.id_unidade
    group by op_unid.id_login
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['p.id_profissional']) }} as id_profissional_sk,
        p.id_profissional,
        ope.id_login,
        coalesce(ope.nome_operador, p.nome) as nome,
        p.cpf,
        p.matricula,
        p.email as email_profissional,
        ope.email as email_operador,
        p.telefone,
        p.flag_ativo,
        ope.login as login_operador,
        ope.tipo_acesso_codigo,
        ope.data_cadastro as data_cadastro_conta,
        ope.data_ultimo_acesso as data_ultimo_acesso,

        -- CBOs (agregados)
        coalesce(ocu.qtde_cbos, 0) as qtde_cbos,
        ocu.codigos_cbo,
        ocu.descricoes_cbo,
        case when coalesce(ocu.qtde_cbos, 0) > 1 then 'Sim' else 'Não' end as flag_multi_cbo,

        -- Perfil de acesso (com a macro map_coluna_perfil_acesso)
        -- Refinamento: nível 8 (Customizado) pode ter subtipo em tipo_acesso_codigo (D=Diretor, M=Master)
        ope.nivel_conta,
        CASE
            WHEN ope.nivel_conta = 8 AND ope.tipo_acesso_codigo = 'D' THEN 'Acesso Personalizado (Diretor)'
            WHEN ope.nivel_conta = 8 AND ope.tipo_acesso_codigo = 'M' THEN 'Acesso Personalizado (Master)'
            ELSE {{ map_coluna_perfil_acesso('ope.nivel_conta') }}
        END AS perfil_acesso,

        -- Status da conta (com a macro map_coluna_status_conta)
        ope.status_conta_codigo,
        {{ map_coluna_status_conta('ope.status_conta_codigo') }},

        -- Dias desde o último acesso
        DATE_DIFF(CURRENT_DATE(), ope.data_ultimo_acesso, DAY) as dias_ultimo_acesso,

        -- Unidades de atuação
        ua.unidades_atuacao,
        coalesce(ua.qtde_unidades, 0) as qtde_unidades

    from profissionais p
    left join operadores ope on p.id_login = ope.id_login
    left join ocupacoes ocu on p.id_profissional = ocu.id_profissional
    left join unidades_atuacao ua on p.id_login = ua.id_login
    where p.nome not like '%TESTE%'
)

select * from final
