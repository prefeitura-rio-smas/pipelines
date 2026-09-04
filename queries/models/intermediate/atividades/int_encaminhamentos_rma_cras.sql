-- Encaminhamentos para o RMA CRAS (Bloco II, itens C2-C5).
-- Pool restrito à regra histórica via pool_evolucoes_ficha (aba CRAS +
-- família explodida em membros). Não restringe int_evolucoes/fct_evolucoes
-- (usados por outros marts).

with pool as (
    select * from {{
        pool_evolucoes_ficha('CRAS - Ficha de Atendimento Individualizado')
    }}
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

limpa_e_extrai as (
    select * from {{
        extrair_encaminhamentos(
            'base',
            [
                'id_evolucao_sk',
                'id_usuario_sk',
                'id_unidade_sk',
                'id_unidade',
                'data_evolucao',
                'nome_usuario'
            ]
        )
    }}
)

select *
from limpa_e_extrai
where (
    encaminhamento_beneficios is not null
    or encaminhamento_orgaos is not null
)
and (nome_usuario not like '%TESTES%' or nome_usuario is null)
