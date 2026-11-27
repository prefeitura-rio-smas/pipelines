{{ config(materialized='ephemeral', tags=['planilha_unificada']) }}
with unidades as (
    select
        id_unidade,
        if(
            count(distinct to_json_string(struct(nome_unidade, cas, email_unidade))) > 1,
            error('Unidade com permissões conflitantes'),
            array_agg(struct(nome_unidade, cas, email_unidade) limit 1)[offset(0)]
        ) as dados
    from {{ ref('raw_unidades') }}
    group by id_unidade
),

planilha as (
    select
        lower(trim(unidade_atendimento)) as nome,
        if(count(distinct to_json_string(struct(email))) > 1, error('Permissões conflitantes na planilha'), max(email)) as email
    from {{ ref('raw_sheets_filtro_email_prontuario') }}
    group by nome
),

nomes as (
    select
        lower(trim(n.dados.nome_unidade)) as nome,
        count(*) as quantidade
    from unidades as n
    group by nome
),

destinatarios as (
    select
        u.id_unidade,
        u.dados.nome_unidade,
        u.dados.email_unidade,
        coalesce({{ email_cas('u.dados.cas') }}, 'gdados.smas@prefeitura.rio') as email_cas,
        p.email as email_planilha
    from unidades as u
    left join planilha as p on lower(trim(u.dados.nome_unidade)) = p.nome
    left join nomes on p.nome = nomes.nome
    where
        u.id_unidade is not null
        and if(p.email is not null and nomes.quantidade > 1, error('Nome da planilha corresponde a mais de uma unidade'), true)
),

emails as (
    select
        id_unidade,
        nome_unidade,
        email_unidade,
        email_cas,
        concat(email_cas, ',', coalesce(email_unidade, ''), ',', coalesce(email_planilha, '')) as email_atendimento
    from destinatarios
)

select
    *,
    coalesce((
        select string_agg(distinct lower(trim(e)), ', ' order by lower(trim(e)))
        from unnest(split(regexp_replace(email_atendimento, r'[;\r\n]', ','), ',')) as e
        where nullif(trim(e), '') is not null
    ), '') as email_filtro
from emails
