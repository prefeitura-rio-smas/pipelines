-- Extração dos campos de encaminhamento (SMAS/Benefícios/Órgãos) do HTML das
-- evoluções via macro genérica extrair_campos_html_evolucao + pivô por label.
with base as (
    select
        e.id_evolucao_sk,
        e.id_usuario_sk,
        e.id_unidade_sk,
        e.descricao_evolucao,
        u.nome as nome_usuario
    from {{ ref('fct_evolucoes') }} as e
    left join {{ ref('dim_usuarios') }} as u on e.id_usuario_sk = u.id_usuario_sk
),

campos as (
    select * from {{ extrair_campos_html_evolucao(
        source_relation = 'base',
        id_cols = ['id_evolucao_sk', 'id_usuario_sk', 'id_unidade_sk', 'nome_usuario']
    ) }}
),

pivotada as (
    select
        id_evolucao_sk,
        id_usuario_sk,
        id_unidade_sk,
        nome_usuario,
        max(case when label like '%Encaminhamentos - %SMAS%' then nullif(valor, '') end) as encaminhamento_smas,
        max(case when label like '%Encaminhamentos - Benefícios%' then nullif(valor, '') end) as encaminhamento_beneficios,
        max(case when label like '%Encaminhamentos Órgãos%' then nullif(valor, '') end) as encaminhamento_orgaos
    from campos
    group by 1, 2, 3, 4
)

select *
from pivotada
where (
    encaminhamento_smas is not null
    or encaminhamento_beneficios is not null
    or encaminhamento_orgaos is not null
)
and (nome_usuario not like '%TESTES%' or nome_usuario is null)
