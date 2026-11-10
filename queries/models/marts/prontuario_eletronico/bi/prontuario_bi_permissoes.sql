{{ config(materialized='table', enabled=target.name != 'prod', tags=['prontuario_bi_unidade']) }}
with unidades_agrupadas as (
    select
        id_unidade,
        if(
            count(distinct to_json_string(struct(cas, email_unidade))) > 1,
            error('E-mails ou CAS conflitantes para a mesma unidade'),
            array_agg(struct(nome_unidade, cas, email_unidade) order by nome_unidade limit 1)[offset(0)]
        ) as dados
    from {{ pu_ref('raw_unidades') }}
    group by id_unidade
),

unidades as (
    select
        dados.*,
        id_unidade
    from unidades_agrupadas
),

nomes as (
    select
        lower(trim(nome_unidade)) as nome,
        count(distinct id_unidade) as quantidade
    from unidades
    group by lower(trim(nome_unidade))
),

planilha as (
    select
        lower(trim(unidade_atendimento)) as nome,
        if(count(distinct to_json_string(struct(email))) > 1, error('E-mails conflitantes para a mesma unidade'), max(email)) as email
    from {{ var('pu_bi_planilha', pu_filtro_email()) }}
    group by lower(trim(unidade_atendimento))
),

listas as (
    select
        u.id_unidade,
        concat(
            coalesce({{ email_cas('u.cas') }}, 'gdados.smas@prefeitura.rio'), ',', coalesce(u.email_unidade, ''), ',',
            if(n.quantidade > 1 and p.email is not null, error('Nome de unidade ambíguo na permissão do BI'), coalesce(p.email, ''))
        ) as emails
    from unidades as u inner join nomes as n on lower(trim(u.nome_unidade)) = n.nome
    left join planilha as p on n.nome = p.nome
)

select distinct
    id_unidade,
    lower(trim(email)) as email
from listas, unnest(split(regexp_replace(emails, r'[;\r\n]', ','), ',')) as email
where id_unidade is not null and nullif(trim(email), '') is not null
