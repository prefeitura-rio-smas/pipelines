{{ config(enabled=target.name != 'prod', materialized='ephemeral', tags=['planilha_unificada']) }}
with unidades as (
    select
        id_unidade,
        if(
            count(distinct to_json_string(struct(nome_unidade, cas, email_unidade))) > 1,
            error('Unidade com permissões conflitantes'),
            array_agg(struct(nome_unidade, cas, email_unidade) limit 1)[offset(0)]
        ) as dados
    from {{ pu_ref('raw_unidades') }} group by id_unidade
),

planilha as (
    select
        lower(trim(unidade_atendimento)) as nome,
        if(count(distinct coalesce(email, '')) > 1, error('Permissões conflitantes na planilha'), max(email)) as email
    from {{ pu_filtro_email() }} group by nome
),

nomes as (
    select
        lower(trim(n.dados.nome_unidade)) as nome,
        count(*) as quantidade
    from unidades as n
    group by nome
)

select
    u.id_unidade,
    u.dados.nome_unidade,
    coalesce((
        select string_agg(distinct lower(trim(e)), ', ' order by lower(trim(e)))
        from
            unnest(split(regexp_replace(concat(
                coalesce({{ email_cas('u.dados.cas') }}, 'gdados.smas@prefeitura.rio'), ',',
                coalesce(u.dados.email_unidade, ''), ',', coalesce(p.email, '')
            ), r'[;\r\n]', ','), ',')) as e
        where nullif(trim(e), '') is not null
    ), '') as email
from unidades as u left join planilha as p on lower(trim(u.dados.nome_unidade)) = p.nome
left join nomes on p.nome = nomes.nome
where
    u.id_unidade is not null
    and if(p.email is not null and nomes.quantidade > 1, error('Nome da planilha corresponde a mais de uma unidade'), true)
