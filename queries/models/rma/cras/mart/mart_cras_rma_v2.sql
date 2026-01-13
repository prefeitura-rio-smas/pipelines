-- Tabela responsável pelos dados finais do RMA CRAS.

with unidades_base as (
    select 
        sequs,
        unidade
    from {{ ref('base_unidades') }}
),

-- Cte responsável pelos itens A1 e A2 do bloco I (RMA CRAS)
total_paif as (
    select
        sequs,
        count(distinct(seqfamil)) as total_famil_paif_sistema_A1,
        count(
            distinct if(
                ano_cadastro_paif = extract(year from current_date())
                and mes_cadastro_paif = extract(month from current_date()),
                seqfamil,
                null
            )
        ) as total_famil_paif_mes_atual_A2
    from {{ ref('int_paif_membros_unidade_responsavel') }}
    group by sequs
),

-- Cte responsável pelos itens B2 e B3 do bloco I (RMA CRAS)
bolsa_familia_e_descumprimento_condicionalidades as (
    select
        sequs,
        count(
            distinct if(
                beneficio = 'Bolsa Família',
                seqfamil,
                null
            )
        ) as total_famil_paif_bf_B2,
        count(
            distinct if(
                seqvulnerab = 1,
                seqfamil,
                null
            )
        ) as total_famil_paif_bf_descumprimento_B3
    from {{ ref('int_paif_membros_unidade_responsavel') }}
    group by sequs
),

beneficiario_bpc as (
    select
        sequs,
        count(
            distinct if (
                beneficio = 'BPC-Benefício de Prestação Continuada',
                seqfamil,
                null
            )
        ) as total_famil_paif_bpc_B4
    from {{ ref('int_paif_membros_unidade_responsavel') }}
    group by sequs
)

select
    a.unidade,
    b.total_famil_paif_sistema_A1,
    b.total_famil_paif_mes_atual_A2,
    c.total_famil_paif_bf_B2,
    c.total_famil_paif_bf_descumprimento_B3,
    d.total_famil_paif_bpc_B4
from unidades_base a
left join total_paif b on a.sequs = b.sequs
left join bolsa_familia_e_descumprimento_condicionalidades c on a.sequs = c.sequs
left join beneficiario_bpc d on a.sequs = d.sequs