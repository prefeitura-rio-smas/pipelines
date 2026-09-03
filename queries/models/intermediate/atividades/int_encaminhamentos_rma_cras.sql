-- Encaminhamentos para o RMA CRAS (Bloco II, itens C2-C5).
-- Escopo restrito à regra histórica: evoluções administrativas de tipo 'F'
-- registradas na aba 'CRAS - Ficha de Atendimento Individualizado', mais
-- evoluções do módulo família atribuídas aos membros ativos da família
-- (o ramo fam original preenche id_usuario = NULL, descartado pelo count distinct).
-- Não restringe int_evolucoes/fct_evolucoes (usados por outros marts).

with adm as (
    select
        e.id_evolucao_sk,
        e.id_usuario_sk,
        e.id_unidade_sk,
        e.id_unidade,
        e.data_evolucao,
        e.descricao_evolucao
    from {{ ref('fct_evolucoes') }} as e
    where
        e.origem_modulo = 'administrativa'
        and e.tipo_evolucao = 'F'
        and regexp_extract(e.descricao_evolucao, r'<h3>(.*?)</h3>')
        = 'CRAS - Ficha de Atendimento Individualizado'
),

fam_expandida as (
    select
        e.id_evolucao_sk,
        e.id_unidade_sk,
        e.id_unidade,
        e.data_evolucao,
        e.descricao_evolucao,
        m.id_paciente as id_usuario
    from {{ ref('fct_evolucoes') }} as e
    inner join {{ ref('raw_membros_familia') }} as m
        on e.id_familia = m.id_familia
    where
        e.origem_modulo = 'familia'
        and m.data_saida is null
),

fam as (
    select
        f.id_evolucao_sk,
        u.id_usuario_sk,
        f.id_unidade_sk,
        f.id_unidade,
        f.data_evolucao,
        f.descricao_evolucao
    from fam_expandida as f
    inner join {{ ref('dim_usuarios') }} as u
        on f.id_usuario = u.id_usuario
),

pool as (
    select * from adm
    union all
    select * from fam
),

base as (
    select
        p.id_evolucao_sk,
        p.id_usuario_sk,
        p.id_unidade_sk,
        p.id_unidade,
        p.data_evolucao,
        p.descricao_evolucao,
        u.nome as nome_usuario
    from pool as p
    left join {{ ref('dim_usuarios') }} as u
        on p.id_usuario_sk = u.id_usuario_sk
),

limpa_html as (
    select
        id_evolucao_sk,
        id_usuario_sk,
        id_unidade_sk,
        id_unidade,
        data_evolucao,
        nome_usuario,
        regexp_replace(descricao_evolucao, '<[^>]+>', ';') as descricao_sem_html
    from base
),

limpa_delimitadores as (
    select
        id_evolucao_sk,
        id_usuario_sk,
        id_unidade_sk,
        id_unidade,
        data_evolucao,
        nome_usuario,
        regexp_replace(descricao_sem_html, ';+', ';') as descricao_limpa
    from limpa_html
),

extrai_encaminhamentos as (
    select
        id_evolucao_sk,
        id_usuario_sk,
        id_unidade_sk,
        id_unidade,
        data_evolucao,
        nome_usuario,
        regexp_extract(
            descricao_limpa,
            r'Encaminhamentos - Benefícios:\s*;?([^;]+?)(?:;Encaminhamentos|;Outros|$)'
        ) as encaminhamento_beneficios,
        regexp_extract(
            descricao_limpa,
            r'Encaminhamentos Órgãos:\s*;?([^;]+?)(?:;Encaminhamentos|;Outros|$)'
        ) as encaminhamento_orgaos
    from limpa_delimitadores
)

select *
from extrai_encaminhamentos
where (
    encaminhamento_beneficios is not null
    or encaminhamento_orgaos is not null
)
and (nome_usuario not like '%TESTES%' or nome_usuario is null)
