with base as (
    select
        e.id_evolucao_sk,
        e.id_usuario_sk,
        e.id_unidade_sk,
        e.descricao_evolucao,
        u.nome as nome_usuario
    from {{ ref('fct_evolucoes') }} e
    left join {{ ref('dim_usuarios') }} u on e.id_usuario_sk = u.id_usuario_sk
),

limpa_html as (
    select
        id_evolucao_sk,
        id_usuario_sk,
        id_unidade_sk,
        nome_usuario,
        regexp_replace(descricao_evolucao, '<[^>]+>', ';') as descricao_sem_html
    from base
),

limpa_delimitadores as (
    select
        id_evolucao_sk,
        id_usuario_sk,
        id_unidade_sk,
        nome_usuario,
        regexp_replace(descricao_sem_html, ';+', ';') as descricao_limpa
    from limpa_html
),

extrai_encaminhamentos as (
    select
        id_evolucao_sk,
        id_usuario_sk,
        id_unidade_sk,
        nome_usuario,
        regexp_extract(
            descricao_limpa,
            r'Encaminhamentos - (?:Atividades )?SMAS:\s*;?([^;]+?)(?:;Encaminhamentos|;Outros|$)'
        ) as encaminhamento_smas,
        regexp_extract(
            descricao_limpa,
            r'Encaminhamentos Órgãos:\s*;?([^;]+?)(?:;Encaminhamentos|;Outros|$)'
        ) as encaminhamento_orgaos
    from limpa_delimitadores
)

select *
from extrai_encaminhamentos
where (
    encaminhamento_smas is not null
    or encaminhamento_orgaos is not null
)
and (nome_usuario not like '%TESTES%' or nome_usuario is null)
