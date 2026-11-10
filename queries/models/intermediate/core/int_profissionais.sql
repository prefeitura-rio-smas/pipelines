-- Camada Intermediate: Entidade Profissionais (input da dim_profissionais)
-- Grão: 1 linha por profissional
-- Inclui dados do operador (conta de login), CBOs agregados,
-- perfil de acesso e status da conta. A surrogate key fica na dim_profissionais.
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
    from {{ ref('raw_profissionais_ocupacoes') }} as poc
    left join {{ ref('raw_cbo') }} as c on poc.codigo_cbo = c.codigo_cbo
),

ocupacoes as (
    select
        id_profissional,
        count(*) as qtde_cbos,
        -- CBOs agregados em ARRAY<STRING> nativo, ordenados por codigo_cbo.
        array_agg(cast(codigo_cbo as string) order by codigo_cbo) as codigos_cbo,
        array_agg(descricao order by codigo_cbo) as descricoes_cbo,
        -- CBO principal (determinístico): menor codigo_cbo do profissional.
        -- Mantido como STRING (elemento único por definição), extraído do array ordenado.
        array_agg(cast(codigo_cbo as string) order by codigo_cbo)[safe_offset(0)] as cbo_principal_codigo,
        array_agg(descricao order by codigo_cbo)[safe_offset(0)] as cbo_principal_descricao
    from profissionais_ocupacoes_detalhadas
    group by id_profissional
),

unidades_atuacao as (
    select
        op_unid.id_login,
        string_agg(u.nome_unidade, ', ' order by u.nome_unidade) as unidades_atuacao,
        count(distinct u.id_unidade) as qtde_unidades,
        array_agg(op_unid.id_unidade order by u.nome_unidade) as ids_unidade
    from {{ ref('raw_operadores_unidades') }} as op_unid
    left join {{ ref('int_unidades') }} as u on op_unid.id_unidade = u.id_unidade
    where u.id_unidade is not null
    group by op_unid.id_login
),

final as (
    select
        p.id_profissional,
        ope.id_login,
        coalesce(ope.nome_operador, p.nome) as nome,
        p.nome as nome_cadastral,
        ope.nome_operador,
        (p.id_login is null) as flag_sem_conta,
        p.cpf,
        p.matricula,
        p.email as email_profissional,
        ope.email as email_operador,
        p.telefone,
        p.flag_ativo,
        ope.login as login_operador,
        ope.tipo_acesso_codigo,
        ope.data_cadastro as data_cadastro_conta,
        ope.data_ultimo_acesso,

        -- CBOs (agregados)
        coalesce(ocu.qtde_cbos, 0) as qtde_cbos,
        ocu.codigos_cbo,
        ocu.descricoes_cbo,
        case when coalesce(ocu.qtde_cbos, 0) > 1 then 'Sim' else 'Não' end as flag_multi_cbo,

        -- CBO principal (determinístico: menor codigo_cbo)
        ocu.cbo_principal_codigo,
        ocu.cbo_principal_descricao,

        -- Perfil de acesso (com a macro map_coluna_perfil_acesso)
        -- Refinamento: nível 8 (Customizado) pode ter subtipo em tipo_acesso_codigo (D=Diretor, M=Master)
        ope.nivel_conta,
        case
            when ope.nivel_conta = 8 and ope.tipo_acesso_codigo = 'D' then 'Acesso Personalizado (Diretor)'
            when ope.nivel_conta = 8 and ope.tipo_acesso_codigo = 'M' then 'Acesso Personalizado (Master)'
            else {{ map_coluna_perfil_acesso('ope.nivel_conta') }}
        end as perfil_acesso,

        -- Perfil de acesso personalizado (nome real via raw_contas_modulos → raw_perfis_grupos)
        pa.id_perfil_acesso,
        pa.nome_perfil as perfil_acesso_nome,

        -- Status da conta (com a macro map_coluna_status_conta)
        ope.status_conta_codigo,
        {{ map_coluna_status_conta('ope.status_conta_codigo') }},

        -- Dias desde o último acesso
        date_diff(current_date(), ope.data_ultimo_acesso, day) as dias_ultimo_acesso,

        -- Unidades de atuação
        ua.unidades_atuacao,
        coalesce(ua.qtde_unidades, 0) as qtde_unidades,
        ua.ids_unidade

    from profissionais as p
    left join operadores as ope on p.id_login = ope.id_login
    left join {{ ref('raw_contas_modulos') }} as cm on ope.id_login = cm.id_login
    left join {{ ref('raw_perfis_grupos') }} as pa on cm.id_perfil_acesso = pa.id_perfil_acesso
    left join ocupacoes as ocu on p.id_profissional = ocu.id_profissional
    left join unidades_atuacao as ua on p.id_login = ua.id_login
    where upper(p.nome) not like '%TESTE%'
)

select * from final
