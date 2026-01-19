-- Tabela responsável pelos dados finais do RMA CRAS.

with unidades_base as (
    select 
        sequs,
        unidade,
        data_extracao
    from {{ ref('raw_unidades') }}
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

-- Cte responsável pelos item B4 do bloco I (RMA CRAS)
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
),

-- Cte responsável pelos item B5 do bloco I (RMA CRAS)
trabalho_infantil_crianca_adoslecente as (
    select
        sequs,
        count(
            distinct if (
                viol_direito = 'Trabalho Infantil'
                and mes_nascimento < extract(month from current_date())
                and date_diff(data_nascimento, current_date(), year) = 18
                and dia_nascimento < extract(day from current_date()),
                seqfamil,
                null
            )
        ) as trab_infantil_crianca_adoslecente_B5      
    from {{ ref('int_paif_membros_unidade_responsavel') }}
    group by sequs
),

-- Cte responsável pelos item C1 do bloco II (RMA CRAS)
atendimentos as (
    select
        unidade,
        count(distinct(seq_atendimento)) as total_atendimentos_C1
    from {{ ref('raw_atendimentos') }}
    group by unidade
),

-- Cte responsável pelos itens C2, C3, C4 e C5 do bloco II (RMA CRAS)
evolucao as (
    select
        sequs,
        count(
            distinct if (
                regexp_contains(encaminhamento_beneficios, '(?i)Cadastro/Atualização Cadúnico'),
                seqpac,
                null
            )
        ) as encaminhamento_cadunico_C2_C3,
        count(
            distinct if (
                regexp_contains(encaminhamento_beneficios, '(?i)BPC - Idoso|BPC - PCD'),
                seqpac,
                null
            )
        ) as encaminhamento_bpc_C4,
        count(
            distinct if (
                regexp_contains(encaminhamento_orgaos, '(?i)CREAS'),
                seqpac,
                null
            )
        ) as encaminhamento_creas_C5,
    from {{ ref('int_evolucao') }}
    group by sequs
)


select
    a.data_extracao,
    a.sequs,
    a.unidade,
    b.total_famil_paif_sistema_A1,
    b.total_famil_paif_mes_atual_A2,
    c.total_famil_paif_bf_B2,
    c.total_famil_paif_bf_descumprimento_B3,
    d.total_famil_paif_bpc_B4,
    e.trab_infantil_crianca_adoslecente_B5,
    f.total_atendimentos_C1,
    g.encaminhamento_cadunico_C2_C3,
    g.encaminhamento_bpc_C4,
    g.encaminhamento_creas_C5
from unidades_base a
left join total_paif b on a.sequs = b.sequs
left join bolsa_familia_e_descumprimento_condicionalidades c on a.sequs = c.sequs
left join beneficiario_bpc d on a.sequs = d.sequs
left join trabalho_infantil_crianca_adoslecente e  on a.sequs = e.sequs
left join atendimentos f  on a.unidade = f.unidade
left join evolucao g  on a.sequs = g.sequs