{{ config(tags = ['daily']) }}

-- RMA CRAS: indicadores mensais por unidade CRAS, convertido para a nova arquitetura.
-- Bloco I (A1, A2, B2, B3, B4, B5): famílias em acompanhamento PAIF.
-- Bloco II (C1, C2_C3, C4, C5, C6): atendimentos e encaminhamentos.

with unidades_cras as (
    select
        id_unidade_sk,
        id_unidade,
        nome_unidade
    from {{ ref('dim_unidades') }}
    where tipo_unidade = 'CRAS'
),

paif as (
    select * from {{ ref('int_paif_membros_unidade') }}
),

-- Itens A1 e A2 do bloco I (RMA CRAS)
total_paif as (
    select
        id_unidade,
        count(distinct id_familia) as total_famil_paif_sistema_a1,
        count(
            distinct if(
                extract(year from data_cadastro_paif) = extract(year from current_date())
                and extract(month from data_cadastro_paif) = extract(month from current_date()),
                id_familia,
                null
            )
        ) as total_famil_paif_mes_atual_a2
    from paif
    group by 1
),

-- Itens B2 e B3 do bloco I (RMA CRAS)
bolsa_familia_e_descumprimento_condicionalidades as (
    select
        p.id_unidade,
        count(
            distinct if(
                b.descricao = 'Bolsa Família'
                and extract(year from p.data_cadastro_paif) = extract(year from current_date())
                and extract(month from p.data_cadastro_paif) = extract(month from current_date()),
                p.id_familia,
                null
            )
        ) as total_famil_paif_bf_b2,
        count(
            distinct if(
                v.id_vulnerabilidade = 1
                and extract(year from p.data_cadastro_paif) = extract(year from current_date())
                and extract(month from p.data_cadastro_paif) = extract(month from current_date()),
                p.id_familia,
                null
            )
        ) as total_famil_paif_descumprimento_b3
    from paif as p
    left join unnest(p.beneficio) as b on true
    left join unnest(p.vulnerabilidades) as v on true
    group by 1
),

-- Item B4 do bloco I (RMA CRAS)
beneficiario_bpc as (
    select
        p.id_unidade,
        count(
            distinct if(
                b.descricao = 'BPC-Benefício de Prestação Continuada'
                and extract(year from p.data_cadastro_paif) = extract(year from current_date())
                and extract(month from p.data_cadastro_paif) = extract(month from current_date()),
                p.id_familia,
                null
            )
        ) as total_famil_paif_bpc_b4
    from paif as p
    left join unnest(p.beneficio) as b on true
    group by 1
),

-- Item B5 do bloco I (RMA CRAS): trabalho infantil (criança/adolescente)
trabalho_infantil_crianca_adolescente as (
    select
        p.id_unidade,
        count(
            distinct if(
                v.descricao = 'Trabalho Infantil'
                and extract(month from p.data_nascimento) < extract(month from current_date())
                and date_diff(current_date(), p.data_nascimento, year) = 18
                and extract(day from p.data_nascimento) < extract(day from current_date())
                and extract(year from p.data_cadastro_paif) = extract(year from current_date())
                and extract(month from p.data_cadastro_paif) = extract(month from current_date()),
                p.id_familia,
                null
            )
        ) as trab_infantil_crianca_adolescente_b5
    from paif as p
    left join unnest(p.violacoes) as v on true
    group by 1
),

-- Item C1 do bloco II (RMA CRAS): atendimentos (exceto recepção)
atendimentos as (
    select
        id_unidade_sk,
        count(distinct id_atendimento_modulo) as total_atendimentos_c1
    from {{ ref('fct_atendimentos') }}
    where
        not regexp_contains(tipo_atendimento_descricao, '(?i)recepção')
        and (flag_cancelado is null or flag_cancelado != 'S')
    group by 1
),

-- Item C6 do bloco II (RMA CRAS): atendimentos domiciliares
atendimentos_domiciliar as (
    select
        id_unidade_sk,
        count(*) as total_atendimentos_domiciliar_c6
    from {{ ref('fct_atendimentos') }}
    where
        regexp_contains(tipo_atendimento_descricao, '(?i)domiciliar')
        and (flag_cancelado is null or flag_cancelado != 'S')
    group by 1
),

-- Itens C2, C3, C4 e C5 do bloco II (RMA CRAS): encaminhamentos
evolucao as (
    select
        id_unidade_sk,
        count(
            distinct if(
                regexp_contains(encaminhamento_beneficios, '(?i)Cadastro/Atualização Cadúnico'),
                id_usuario_sk,
                null
            )
        ) as encaminhamento_cadunico_c2_c3,
        count(
            distinct if(
                regexp_contains(encaminhamento_beneficios, '(?i)BPC - Idoso|BPC - PCD'),
                id_usuario_sk,
                null
            )
        ) as encaminhamento_bpc_c4,
        count(
            distinct if(
                regexp_contains(encaminhamento_orgaos, '(?i)CREAS'),
                id_usuario_sk,
                null
            )
        ) as encaminhamento_creas_c5
    from {{ ref('int_encaminhamentos_evolucoes') }}
    group by 1
)

select
    du.id_unidade_sk,
    du.id_unidade,
    du.nome_unidade,
    coalesce(tp.total_famil_paif_sistema_a1, 0) as total_famil_paif_sistema_a1,
    coalesce(tp.total_famil_paif_mes_atual_a2, 0) as total_famil_paif_mes_atual_a2,
    coalesce(bf.total_famil_paif_bf_b2, 0) as total_famil_paif_bf_b2,
    coalesce(bf.total_famil_paif_descumprimento_b3, 0) as total_famil_paif_descumprimento_b3,
    coalesce(bpc.total_famil_paif_bpc_b4, 0) as total_famil_paif_bpc_b4,
    coalesce(ti.trab_infantil_crianca_adolescente_b5, 0) as trab_infantil_crianca_adolescente_b5,
    coalesce(atd.total_atendimentos_c1, 0) as total_atendimentos_c1,
    coalesce(ev.encaminhamento_cadunico_c2_c3, 0) as encaminhamento_cadunico_c2_c3,
    coalesce(ev.encaminhamento_bpc_c4, 0) as encaminhamento_bpc_c4,
    coalesce(ev.encaminhamento_creas_c5, 0) as encaminhamento_creas_c5,
    coalesce(adm.total_atendimentos_domiciliar_c6, 0) as total_atendimentos_domiciliar_c6,
    {{ extrair_ultima_atualizacao('raw_configuracoes_sistema') }} as ultima_atualizacao
from unidades_cras as du
left join total_paif as tp on du.id_unidade = tp.id_unidade
left join bolsa_familia_e_descumprimento_condicionalidades as bf on du.id_unidade = bf.id_unidade
left join beneficiario_bpc as bpc on du.id_unidade = bpc.id_unidade
left join trabalho_infantil_crianca_adolescente as ti on du.id_unidade = ti.id_unidade
left join atendimentos as atd on du.id_unidade_sk = atd.id_unidade_sk
left join atendimentos_domiciliar as adm on du.id_unidade_sk = adm.id_unidade_sk
left join evolucao as ev on du.id_unidade_sk = ev.id_unidade_sk
