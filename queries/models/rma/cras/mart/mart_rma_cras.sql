-- CTE para filtrar os itens C2, C3, C4 e C5 do item C do bloco II ( RMA - CRAS)
with total_unidade_evol as (
    select
        sequs,
        count(atividades_smas) as encam_cadunico,
        count(beneficios) as encam_bpc,
        count(orgaos) as encam_creas
    from {{ ref('int_evolucaofamil') }}
    group by sequs
),

-- CTE para filtrar os itens B2, B3 e B4 do Item B bloco I ( RMA - CRAS )
total_benef_vulnerab as (
    select
        unidade,
        countif(beneficio = 'BPC-Benefício de Prestação Continuada') as total_bpc,
        countif(beneficio = 'Bolsa Família') as total_bolsa_familia,
        countif(seqvulnerab = 1) as total_descumprimento_cond_bf
    from {{ ref('int_benef_e_vulnerab') }}
    group by unidade
),

-- Cte para filtrar total de familias que estão no acompanhamento PAIF e novas famílias inseridas no mês filtrado
total_paif as (
    select
        unidade,
        count(*) as total_famil_paif_sistema,
        countif(mes_cadastro = 12 and ano_cadastro = 2025) as total_familia_paif_mes_dez_2025
    from {{ref('int_paif_famil')}}
    group by unidade

),

-- Cte para filtrar todos os atendimentos de cada unidade
atendimentos as (
    select
        unidade_atendimento,
        count(distinct(seq_atendimento)) as total_atendimentos
    from {{ source('cras_rma_dev', 'dev_atendimentos')}}
    group by unidade_atendimento
),

final as (
    select
        a.sequs,
        a.dscus,
        b.total_famil_paif_sistema,
        b.total_familia_paif_mes_dez_2025,
        c.total_bpc,
        c.total_bolsa_familia,
        c.total_descumprimento_cond_bf,
        d.encam_cadunico,
        d.encam_bpc,
        d.encam_creas,
        e.total_atendimentos
    from {{ source('cras_rma_prod','gh_us') }} a
    left join total_paif b on a.dscus = b.unidade
    left join total_benef_vulnerab c on a.dscus = c.unidade
    left join total_unidade_evol d on a.sequs = d.sequs
    left join atendimentos e on a.dscus = e.unidade_atendimento
) 

select * from final