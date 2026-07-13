with unidades_piloto as (
    select r'ismenia de lima martins' as pattern, 'CRAS PROF. ISMÊNIA DE LIMA MARTINS' as nome_canonico
    union all select r'rosani cunha', 'CRAS ROSANI CUNHA'
    union all select r'sobral pinto', 'CRAS SOBRAL PINTO'
    union all select r'darcy ribeiro', 'CRAS PROF. DARCY RIBEIRO'
    union all select r'jose carlos campos', 'CRAS JOSÉ CARLOS CAMPOS'
    union all select r'\bacari\b', 'CRAS ACARI'
    union all select r'machado de assis', 'CRAS MACHADO DE ASSIS'
    union all select r'vila moretti', 'CRAS VILA MORETTI'
    union all select r'marcelo cardoso tome', 'CRAS MARCELO CARDOSO TOMÉ'
    union all select r'helenice nunes jacinth?', 'CRAS HELENICE NUNES JACINTO'
    union all select r'maria lina de castro lima', 'CREAS MARIA LINA DE CASTRO LIMA'
    union all select r'stella maris', 'CREAS STELLA MARIS'
),

base_unidades as (
    select 
        du.id_unidade_sk,
        up.nome_canonico,
        du.nome_unidade as nome_banco
    from {{ ref('dim_unidades') }} du
    inner join unidades_piloto up on regexp_contains(
        lower(regexp_replace(normalize(du.nome_unidade, nfd), r'\p{M}', '')),
        up.pattern
    )
),

-- 1, 4 e 7: PAIF, PAEFI e MSE (Serviços das Famílias)
servicos_familias as (
    select
        f.id_unidade,
        count(distinct if(s.id_servico_assistencial = 1, f.id_familia, null)) as total_paif,
        count(distinct if(s.id_servico_assistencial = 6, f.id_familia, null)) as total_paefi,
        count(distinct if(s.id_servico_assistencial in (8, 9), f.id_familia, null)) as total_mse
    from {{ ref('raw_familias') }} f
    inner join {{ ref('raw_familias_servicos_assistenciais') }} s on f.id_familia = s.id_familia
    where s.data_cancelamento is null
    group by 1
),

-- 6: Situação de Rua (vinda da dim_usuarios baseada em atendimentos)
situacao_rua as (
    select
        id_unidade,
        count(distinct id_usuario) as total_rua
    from {{ ref('fct_atendimentos') }}
    where id_usuario in (select id_usuario from {{ ref('dim_usuarios') }} where flag_situacao_rua = 'Sim')
      and (flag_cancelado is null or flag_cancelado != 'S')
    group by 1
),

-- 3 e 8: Encaminhamentos (vinda das evoluções)
encaminhamentos_evolucoes as (
    select
        id_unidade_sk,
        count(distinct if(regexp_contains(encaminhamento_smas, r'(?i)CRAS'), id_usuario_sk, null)) as encaminhamento_cras,
        count(distinct if(regexp_contains(encaminhamento_orgaos, r'(?i)CREAS'), id_usuario_sk, null)) as encaminhamento_creas
    from {{ ref('int_encaminhamentos_evolucoes') }}
    group by 1
)

-- Join Final Consolidado
select
    b.nome_canonico,
    -- Indicadores
    coalesce(sf.total_paif, 0) as total_paif,
    coalesce(sf.total_paefi, 0) as total_paefi,
    coalesce(sf.total_mse, 0) as total_mse,
    coalesce(sr.total_rua, 0) as total_rua,
    coalesce(ee.encaminhamento_cras, 0) as encaminhamento_cras,
    coalesce(ee.encaminhamento_creas, 0) as encaminhamento_creas
from base_unidades b
left join {{ ref('dim_unidades') }} du on b.id_unidade_sk = du.id_unidade_sk
left join servicos_familias sf on du.id_unidade = sf.id_unidade
left join situacao_rua sr on du.id_unidade = sr.id_unidade
left join encaminhamentos_evolucoes ee on b.id_unidade_sk = ee.id_unidade_sk
