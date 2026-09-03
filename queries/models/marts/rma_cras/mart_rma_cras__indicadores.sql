{{ config(tags = ['daily']) }}

-- RMA CRAS: indicadores do mês de referência por unidade CRAS, nova arquitetura.
-- Bloco I (A1, A2, B1, B2, B3, B4, B5, B6): famílias em acompanhamento PAIF.
-- Bloco II (C1, C2_C3, C4, C5, C6): atendimentos individualizados e encaminhamentos.
-- Bloco D (D.1-D.7): serviços de convivência e atividades coletivas.
-- Mês de referência: var competencia ('AAAA-MM'); vazio = mês corrente.
-- C2 = C3 por construção da fonte (checkbox único 'Cadastro/Atualização Cadúnico').

{% set corte_ep = var('corte_extrema_pobreza', 218) %}

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

paif_novas as (
    select
        id_familia,
        id_unidade,
        id_usuario,
        data_nascimento,
        beneficio,
        violacoes,
        vulnerabilidades
    from paif
    where {{ no_mes('data_cadastro_paif') }}
),

-- Itens A1 e A2 do bloco I (RMA CRAS)
total_paif as (
    select
        id_unidade,
        count(distinct id_familia) as total_famil_paif_sistema_a1,
        count(
            distinct if(
                {{ no_mes('data_cadastro_paif') }},
                id_familia,
                null
            )
        ) as total_famil_paif_mes_atual_a2
    from paif
    group by 1
),

-- Item B1 do bloco I (RMA CRAS): extrema pobreza via CadÚnico (renda per capita)
extrema_pobreza as (
    select
        p.id_unidade,
        count(
            distinct if(
                ep.renda_media_pc <= {{ corte_ep }},
                p.id_familia,
                null
            )
        ) as total_famil_paif_extrema_pobreza_b1
    from paif_novas as p
    inner join {{ ref('int_familias_extrema_pobreza') }} as ep
        on p.id_familia = ep.id_familia
    group by 1
),

-- Itens B2 e B3 do bloco I (RMA CRAS)
bolsa_familia_e_descumprimento_condicionalidades as (
    select
        p.id_unidade,
        count(
            distinct if(
                b.descricao = 'Bolsa Família',
                p.id_familia,
                null
            )
        ) as total_famil_paif_bf_b2,
        count(
            distinct if(
                v.id_vulnerabilidade = 1,
                p.id_familia,
                null
            )
        ) as total_famil_paif_descumprimento_b3
    from paif_novas as p
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
                b.descricao = 'BPC-Benefício de Prestação Continuada',
                p.id_familia,
                null
            )
        ) as total_famil_paif_bpc_b4
    from paif_novas as p
    left join unnest(p.beneficio) as b on true
    group by 1
),

-- Item B5 do bloco I (RMA CRAS): trabalho infantil (membro < 18 anos no fim do mês)
trabalho_infantil_crianca_adolescente as (
    select
        p.id_unidade,
        count(
            distinct if(
                v.descricao = 'Trabalho Infantil'
                and {{ calc_idade('p.data_nascimento', 'last_day(' ~ mes_referencia() ~ ')') }} < 18,
                p.id_familia,
                null
            )
        ) as trab_infantil_crianca_adolescente_b5
    from paif_novas as p
    left join unnest(p.violacoes) as v on true
    group by 1
),

-- Item B6 do bloco I (RMA CRAS): criança/adolescente em Serviço de Acolhimento
ciclos_abertos as (
    select id_usuario
    from {{ ref('raw_usuarios_acolhimentos') }}
    where data_saida is null
),

acolhimento as (
    select
        p.id_unidade,
        count(
            distinct if(
                c.id_usuario is not null
                and {{ calc_idade('p.data_nascimento', 'last_day(' ~ mes_referencia() ~ ')') }} < 18,
                p.id_familia,
                null
            )
        ) as total_famil_paif_acolhimento_b6
    from paif_novas as p
    left join ciclos_abertos as c on p.id_usuario = c.id_usuario
    group by 1
),

-- Item C1 do bloco II (RMA CRAS): atendimentos (exceto recepção) no mês
atendimentos as (
    select
        id_unidade_sk,
        count(distinct id_atendimento_modulo) as total_atendimentos_c1
    from {{ ref('fct_atendimentos') }}
    where
        not regexp_contains(tipo_atendimento_descricao, '(?i)recepção')
        and (flag_cancelado is null or flag_cancelado != 'S')
        and {{ no_mes('data_atendimento') }}
    group by 1
),

-- Item C6 do bloco II (RMA CRAS): atendimentos domiciliares no mês
atendimentos_domiciliar as (
    select
        id_unidade_sk,
        count(*) as total_atendimentos_domiciliar_c6
    from {{ ref('fct_atendimentos') }}
    where
        regexp_contains(tipo_atendimento_descricao, '(?i)domiciliar')
        and (flag_cancelado is null or flag_cancelado != 'S')
        and {{ no_mes('data_atendimento') }}
    group by 1
),

-- Itens C2, C3, C4 e C5 do bloco II (RMA CRAS): encaminhamentos no mês
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
    from {{ ref('int_encaminhamentos_rma_cras') }}
    where {{ no_mes('data_evolucao') }}
    group by 1
),

-- Bloco D (RMA CRAS): atividades de grupo ocorrem em polos, não nos CRAS;
-- a atribuição por unidade usa o vínculo PAIF do participante
paif_membros as (
    select distinct
        id_familia,
        id_usuario,
        id_unidade
    from paif
),

coletivo_membros as (
    select
        p.id_unidade,
        p.id_familia,
        c.id_usuario,
        c.idade_anos,
        c.id_tipo_atividade,
        c.flag_deficiencia
    from {{ ref('int_coletivos_mes') }} as c
    inner join paif_membros as p
        on c.id_usuario = p.id_usuario
),

coletivos as (
    select
        id_unidade,
        count(
            distinct if(
                idade_anos between 0 and 6,
                id_usuario,
                null
            )
        ) as total_criancas_0_6_d2,
        count(
            distinct if(
                idade_anos between 7 and 14
                and id_tipo_atividade in (1, 2),
                id_usuario,
                null
            )
        ) as total_criancas_7_14_scfv_d3,
        count(
            distinct if(
                idade_anos between 15 and 17
                and id_tipo_atividade = 3,
                id_usuario,
                null
            )
        ) as total_adolescentes_15_17_scfv_d4,
        count(
            distinct if(
                idade_anos >= 60
                and id_tipo_atividade = 4,
                id_usuario,
                null
            )
        ) as total_idosos_scfv_d5,
        count(
            distinct if(
                id_tipo_atividade in (5, 6, 7, 8),
                id_usuario,
                null
            )
        ) as total_participantes_nao_continuado_d6,
        count(
            distinct if(
                flag_deficiencia = 'S',
                id_usuario,
                null
            )
        ) as total_pcd_coletivos_d7
    from coletivo_membros
    group by 1
),

-- Item D.1 (RMA CRAS): famílias PAIF com membro presente em grupo no mês
paif_coletivos as (
    select
        id_unidade,
        count(distinct id_familia) as total_familias_grupos_paif_d1
    from coletivo_membros
    group by 1
)

select
    du.id_unidade_sk,
    du.id_unidade,
    du.nome_unidade,
    coalesce(tp.total_famil_paif_sistema_a1, 0) as total_famil_paif_sistema_a1,
    coalesce(tp.total_famil_paif_mes_atual_a2, 0) as total_famil_paif_mes_atual_a2,
    coalesce(ep.total_famil_paif_extrema_pobreza_b1, 0) as total_famil_paif_extrema_pobreza_b1,
    coalesce(bf.total_famil_paif_bf_b2, 0) as total_famil_paif_bf_b2,
    coalesce(bf.total_famil_paif_descumprimento_b3, 0) as total_famil_paif_descumprimento_b3,
    coalesce(bpc.total_famil_paif_bpc_b4, 0) as total_famil_paif_bpc_b4,
    coalesce(ti.trab_infantil_crianca_adolescente_b5, 0) as trab_infantil_crianca_adolescente_b5,
    coalesce(ac.total_famil_paif_acolhimento_b6, 0) as total_famil_paif_acolhimento_b6,
    coalesce(atd.total_atendimentos_c1, 0) as total_atendimentos_c1,
    coalesce(ev.encaminhamento_cadunico_c2_c3, 0) as encaminhamento_cadunico_c2_c3,
    coalesce(ev.encaminhamento_bpc_c4, 0) as encaminhamento_bpc_c4,
    coalesce(ev.encaminhamento_creas_c5, 0) as encaminhamento_creas_c5,
    coalesce(adm.total_atendimentos_domiciliar_c6, 0) as total_atendimentos_domiciliar_c6,
    coalesce(pc.total_familias_grupos_paif_d1, 0) as total_familias_grupos_paif_d1,
    coalesce(col.total_criancas_0_6_d2, 0) as total_criancas_0_6_d2,
    coalesce(col.total_criancas_7_14_scfv_d3, 0) as total_criancas_7_14_scfv_d3,
    coalesce(col.total_adolescentes_15_17_scfv_d4, 0) as total_adolescentes_15_17_scfv_d4,
    coalesce(col.total_idosos_scfv_d5, 0) as total_idosos_scfv_d5,
    coalesce(col.total_participantes_nao_continuado_d6, 0) as total_participantes_nao_continuado_d6,
    coalesce(col.total_pcd_coletivos_d7, 0) as total_pcd_coletivos_d7,
    {{ extrair_ultima_atualizacao('raw_configuracoes_sistema') }} as ultima_atualizacao
from unidades_cras as du
left join total_paif as tp on du.id_unidade = tp.id_unidade
left join extrema_pobreza as ep on du.id_unidade = ep.id_unidade
left join bolsa_familia_e_descumprimento_condicionalidades as bf on du.id_unidade = bf.id_unidade
left join beneficiario_bpc as bpc on du.id_unidade = bpc.id_unidade
left join trabalho_infantil_crianca_adolescente as ti on du.id_unidade = ti.id_unidade
left join acolhimento as ac on du.id_unidade = ac.id_unidade
left join atendimentos as atd on du.id_unidade_sk = atd.id_unidade_sk
left join atendimentos_domiciliar as adm on du.id_unidade_sk = adm.id_unidade_sk
left join evolucao as ev on du.id_unidade_sk = ev.id_unidade_sk
left join coletivos as col on du.id_unidade = col.id_unidade
left join paif_coletivos as pc on du.id_unidade = pc.id_unidade
