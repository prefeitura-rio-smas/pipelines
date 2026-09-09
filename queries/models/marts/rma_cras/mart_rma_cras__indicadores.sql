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

-- PAIF: famílias vinculadas ao serviço 1 (PAIF), membros ativos, com
-- atribuição de unidade em 3 níveis (serviço → operador → atendimento mais
-- recente no CRAS). Legado usava apenas o operador.
paif as (
    select
        p.id_familia,
        p.data_cadastro_servico as data_cadastro_paif,
        m.id_usuario,
        u.data_nascimento,
        u.beneficio,
        u.violacoes,
        vf.vulnerabilidades,
        coalesce(p.id_unidade, ul.id_unidade, af.id_unidade) as id_unidade
    from (
        select
            id_familia,
            id_unidade,
            id_login_cadastro,
            data_cadastro as data_cadastro_servico
        from {{ ref('raw_familias_servicos_assistenciais') }}
        where
            id_servico_assistencial = 1
            and data_cancelamento is null
    ) as p
    inner join (
        select
            id_familia,
            id_paciente as id_usuario
        from {{ ref('raw_membros_familia') }}
        where data_saida is null
    ) as m on p.id_familia = m.id_familia
    inner join (
        select
            id_usuario,
            data_nascimento,
            beneficio,
            violacoes
        from {{ ref('dim_usuarios') }}
    ) as u on m.id_usuario = u.id_usuario
    left join (
        select
            id_familia,
            array_agg(
                struct(
                    id_vulnerabilidade,
                    data_cadastro
                )
            ) as vulnerabilidades
        from {{ ref('raw_familias_vulnerabilidades') }}
        where data_cancelamento is null
        group by id_familia
    ) as vf on p.id_familia = vf.id_familia
    left join (
        select
            id_login,
            min(id_unidade) as id_unidade
        from {{ ref('raw_operadores_unidades') }}
        group by id_login
    ) as ul on p.id_login_cadastro = ul.id_login
    left join (
        select
            a.id_familia,
            array_agg(a.id_unidade order by a.data_atendimento desc, a.id_unidade asc limit 1)[safe_offset(0)] as id_unidade
        from {{ ref('raw_atendimentos_familias') }} as a
        inner join {{ ref('dim_unidades') }} as d
            on
                a.id_unidade = d.id_unidade
                and d.tipo_unidade = 'CRAS'
        where {{ nao_cancelado('a.flag_cancelado') }}
        group by a.id_familia
    ) as af on p.id_familia = af.id_familia
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
                and {{ calc_idade('p.data_nascimento', 'fim_do_mes') }} < 18,
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
                and {{ calc_idade('p.data_nascimento', 'fim_do_mes') }} < 18,
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
        and {{ nao_cancelado() }}
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
        and {{ nao_cancelado() }}
        and {{ no_mes('data_atendimento') }}
    group by 1
),

-- Itens C2, C3, C4 e C5 do bloco II (RMA CRAS): encaminhamentos no mês.
-- Pool restrito à regra histórica: evoluções da aba 'CRAS - Ficha de
-- Atendimento Individualizado' + família explodida em membros.
-- C2/C3/C5 têm grão família (definição oficial RMA): contam famílias
-- encaminhadas. id_familia vem da evolução (ramo família) ou, quando a ficha
-- adm não referencia família, cai para o indivíduo (1 usuário = 1 família).
-- C4 mantém grão indivíduo (definição oficial conta INDIVÍDUOS p/ BPC).
pool_evolucoes as (
    select * from {{
        pool_evolucoes_ficha('CRAS - Ficha de Atendimento Individualizado')
    }}
),

base_evolucoes as (
    select
        p.id_evolucao_sk,
        p.id_usuario_sk,
        p.id_familia,
        p.id_unidade_sk,
        p.id_unidade,
        p.data_evolucao,
        p.descricao_evolucao,
        u.nome as nome_usuario
    from pool_evolucoes as p
    left join {{ ref('dim_usuarios') }} as u
        on p.id_usuario_sk = u.id_usuario_sk
),

encaminhamentos_evolucoes as (
    select * from {{
        extrair_encaminhamentos(
            'base_evolucoes',
            [
                'id_evolucao_sk',
                'id_usuario_sk',
                'id_familia',
                'id_unidade_sk',
                'id_unidade',
                'data_evolucao',
                'nome_usuario'
            ]
        )
    }}
),

evolucao as (
    select
        id_unidade_sk,
        count(
            distinct if(
                regexp_contains(encaminhamento_beneficios, '(?i)Cadastro/Atualização Cadúnico'),
                coalesce(cast(id_familia as string), id_usuario_sk),
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
                coalesce(cast(id_familia as string), id_usuario_sk),
                null
            )
        ) as encaminhamento_creas_c5
    from encaminhamentos_evolucoes
    where
        {{ no_mes('data_evolucao') }}
        and (encaminhamento_beneficios is not null or encaminhamento_orgaos is not null)
        and (nome_usuario not like '%TESTES%' or nome_usuario is null)
    group by 1
),

-- Bloco D (RMA CRAS): atividades de grupo ocorrem em polos, não nos CRAS;
-- a atribuição por unidade usa o vínculo PAIF do participante.
-- Fonte: fct_presencas_usuarios (presenças) + dim_atividades_grupo (tipo) +
-- dim_usuarios (idade, deficiência).
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
    from (
        select
            pr.id_unidade,
            pr.id_usuario,
            a.id_tipo_atividade,
            a.nome_tipo_atividade,
            {{ calc_idade('u.data_nascimento', 'fim_do_mes') }} as idade_anos,
            u.flag_deficiencia
        from {{ ref('fct_presencas_usuarios') }} as pr
        left join {{ ref('dim_atividades_grupo') }} as a
            on pr.id_atividade = a.id_atividade
        left join {{ ref('dim_usuarios') }} as u
            on pr.id_usuario = u.id_usuario
        where {{ no_mes('pr.data_presenca') }}
    ) as c
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
