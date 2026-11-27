{{ config(enabled=target.name in ["dev", "ci"]) }}

with profissionais as (
    select * from {{ ref('raw_profissionais') }}
),

profissionais_ocupacoes_detalhadas as (
    select
        poc.id_profissional,
        poc.codigo_cbo,
        cbo.descricao
    from {{ ref('raw_profissionais_ocupacoes') }} as poc
    left join {{ ref('raw_cbo') }} as cbo on poc.codigo_cbo = cbo.codigo_cbo
    where poc.codigo_cbo is not null
),

ocupacoes as (
    select
        id_profissional,
        count(*) as qtde_cbos,
        -- CBOs agregados em ARRAY<STRING> nativo, ordenados por codigo_cbo.
        array_agg(cast(codigo_cbo as string) order by codigo_cbo) as codigos_cbo,
        array_agg(coalesce(descricao, '') order by codigo_cbo) as descricoes_cbo,
        -- CBO principal (determinístico): menor codigo_cbo do profissional.
        -- Mantido como STRING (elemento único por definição), extraído do array ordenado.
        array_agg(cast(codigo_cbo as string) order by codigo_cbo)[safe_offset(0)] as cbo_principal_codigo,
        array_agg(descricao order by codigo_cbo)[safe_offset(0)] as cbo_principal_descricao
    from profissionais_ocupacoes_detalhadas
    group by id_profissional
)

select
    p.id_profissional,
    p.id_login,
    p.nome,
    p.cpf,
    p.matricula,
    p.email as email_profissional,
    p.telefone,
    p.flag_ativo,

    -- CBOs (agregados)
    ocu.codigos_cbo,
    ocu.descricoes_cbo,

    -- CBO principal (determinístico: menor codigo_cbo)
    ocu.cbo_principal_codigo,
    ocu.cbo_principal_descricao,

    -- Perfil de acesso personalizado (nome real via raw_contas_modulos → raw_perfis_grupos)
    pa.id_perfil_acesso,
    pa.nome_perfil as perfil_acesso_nome,

    (p.id_login is null) as flag_sem_conta,
    coalesce(ocu.qtde_cbos, 0) as qtde_cbos,
    case when coalesce(ocu.qtde_cbos, 0) > 1 then 'Sim' else 'Não' end as flag_multi_cbo

from profissionais as p
left join ocupacoes as ocu on p.id_profissional = ocu.id_profissional
left join {{ ref('raw_contas_modulos') }} as cm on p.id_login = cm.id_login
left join {{ ref('raw_perfis_grupos') }} as pa on cm.id_perfil_acesso = pa.id_perfil_acesso
where upper(p.nome) not like '%TESTE%'
