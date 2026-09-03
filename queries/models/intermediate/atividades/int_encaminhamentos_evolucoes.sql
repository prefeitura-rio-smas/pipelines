-- Extração dos campos de encaminhamento via macro extrair_encaminhamentos
-- (a classificação SMAS/Órgãos/Benefícios é permanente).
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

extraida as (
    select * from {{
        extrair_encaminhamentos(
            'base',
            [
                'id_evolucao_sk',
                'id_usuario_sk',
                'id_unidade_sk',
                'nome_usuario'
            ]
        )
    }}
)

select *
from extraida
where (
    encaminhamento_smas is not null
    or encaminhamento_beneficios is not null
    or encaminhamento_orgaos is not null
)
and (nome_usuario not like '%TESTES%' or nome_usuario is null)
