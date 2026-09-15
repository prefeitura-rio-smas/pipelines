{{ config(tags = ['daily']) }}

-- RMA CRAS: indicadores do mês de referência por unidade CRAS (nova arquitetura).
-- Bloco I  (A1, A2, B1-B6): famílias em acompanhamento PAIF.
-- Bloco II (C1-C6): atendimentos individualizados e encaminhamentos.
-- Bloco D  (D.1-D.7): convivência e atividades coletivas.
-- Referência: var competencia ('AAAA-MM'); vazio = mês corrente.
-- C2 = C3 por construção da fonte (checkbox único 'Cadastro/Atualização Cadúnico').

{% set corte_ep = var('corte_extrema_pobreza', 218) %}

with
-- ============================================================================
-- 0. Dimensão de unidades CRAS
-- ============================================================================
unidades_cras as (
    select
        id_unidade_sk,
        id_unidade,
        nome_unidade
    from {{ ref('dim_unidades') }}
    where tipo_unidade = 'CRAS'
),

-- ============================================================================
-- I. Bloco I — famílias em acompanhamento PAIF
-- ============================================================================
-- I.1 extração: vínculo da família ao serviço PAIF, membros ativos, unidade
membros_ativos as (
    select
        id_familia,
        id_paciente as id_usuario
    from {{ ref('raw_membros_familia') }}
    where data_saida is null
),

usuarios as (
    select
        id_usuario,
        data_nascimento,
        beneficio,
        violacoes
    from {{ ref('dim_usuarios') }}
),

vulnerabilidades_familia as (
    select
        id_familia,
        array_agg(struct(id_vulnerabilidade, data_cadastro)) as vulnerabilidades
    from {{ ref('raw_familias_vulnerabilidades') }}
    where data_cancelamento is null
    group by 1
),

operadores_unidades as (
    select
        id_login,
        min(id_unidade) as id_unidade
    from {{ ref('raw_operadores_unidades') }}
    group by 1
),

-- 3º nível de atribuição de unidade: atendimento mais recente no CRAS
unidade_atendimento_recente as (
    select
        a.id_familia,
        array_agg(a.id_unidade order by a.data_atendimento desc, a.id_unidade asc limit 1)[safe_offset(0)] as id_unidade
    from {{ ref('raw_atendimentos_familias') }} as a
    inner join {{ ref('dim_unidades') }} as d
        on a.id_unidade = d.id_unidade and d.tipo_unidade = 'CRAS'
    where {{ nao_cancelado('a.flag_cancelado') }}
    group by 1
),

-- I.2 transformação: família PAIF + membros + unidade (serviço -> operador -> atendimento)
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
        where id_servico_assistencial = 1 and data_cancelamento is null
    ) as p
    inner join membros_ativos as m on p.id_familia = m.id_familia
    inner join usuarios as u on m.id_usuario = u.id_usuario
    left join vulnerabilidades_familia as vf on p.id_familia = vf.id_familia
    left join operadores_unidades as ul on p.id_login_cadastro = ul.id_login
    left join unidade_atendimento_recente as af on p.id_familia = af.id_familia
),

-- recorte do mês: famílias NOVAS no PAIF no mês de referência
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

-- I.3 agregação: A1/A2
agg_paif as (
    select
        id_unidade,
        {{ contar('id_familia') }} as total_famil_paif_sistema_a1,
        {{ contar('id_familia', no_mes('data_cadastro_paif')) }} as total_famil_paif_mes_atual_a2
    from paif
    group by 1
),

-- B1: famílias novas com renda per capita CadÚnico <= corte
agg_extrema_pobreza as (
    select
        p.id_unidade,
        {{ contar('p.id_familia', 'ep.renda_media_pc <= ' ~ corte_ep) }} as total_famil_paif_extrema_pobreza_b1
    from paif_novas as p
    inner join {{ ref('int_familias_extrema_pobreza') }} as ep on p.id_familia = ep.id_familia
    group by 1
),

-- B2/B3/B4 (benefícios) e B5 (trabalho infantil), mesmo grão de família.
agg_beneficios_vulnerabilidades as (
    select
        p.id_unidade,
        {{ contar('p.id_familia', "ben.descricao = 'Bolsa Família'") }} as total_famil_paif_bf_b2,
        {{ contar('p.id_familia', "vul.id_vulnerabilidade = 1 and ben.descricao = 'Bolsa Família'") }} as total_famil_paif_descumprimento_b3,
        {{ contar('p.id_familia', "ben.descricao = 'BPC-Benefício de Prestação Continuada'") }} as total_famil_paif_bpc_b4,
        {{ contar('p.id_familia', "vio.descricao = 'Trabalho Infantil' and " ~ calc_idade('p.data_nascimento', 'fim_do_mes') ~ " < 18") }} as trab_infantil_crianca_adolescente_b5
    from paif_novas as p
    left join unnest(p.beneficio) as ben on true
    left join unnest(p.vulnerabilidades) as vul on true
    left join unnest(p.violacoes) as vio on true
    group by 1
),

-- B6: criança/adolescente em ciclo de acolhimento aberto
ciclos_abertos as (
    select id_usuario
    from {{ ref('raw_usuarios_acolhimentos') }}
    where data_saida is null
),

agg_acolhimento as (
    select
        p.id_unidade,
        {{ contar('p.id_familia', "c.id_usuario is not null and " ~ calc_idade('p.data_nascimento', 'fim_do_mes') ~ " < 18") }} as total_famil_paif_acolhimento_b6
    from paif_novas as p
    left join ciclos_abertos as c on p.id_usuario = c.id_usuario
    group by 1
),

-- ============================================================================
-- II. Bloco II — atendimentos individualizados e encaminhamentos
-- ============================================================================
-- C1: atendimentos (exceto recepção) no mês
agg_atendimentos as (
    select
        id_unidade_sk,
        {{ contar('id_atendimento_modulo') }} as total_atendimentos_c1
    from {{ ref('fct_atendimentos') }}
    where
        not regexp_contains(tipo_atendimento_descricao, '(?i)recepção')
        and {{ nao_cancelado() }}
        and {{ no_mes('data_atendimento') }}
    group by 1
),

-- C6: atendimentos domiciliares no mês
agg_atendimentos_domiciliar as (
    select
        id_unidade_sk,
        {{ contar('*', none, false) }} as total_atendimentos_domiciliar_c6
    from {{ ref('fct_atendimentos') }}
    where
        regexp_contains(tipo_atendimento_descricao, '(?i)domiciliar')
        and {{ nao_cancelado() }}
        and {{ no_mes('data_atendimento') }}
    group by 1
),

-- Pool elegível a RMA: evoluções tipo 'F' da ficha CRAS (ramo administrativo)
-- + evoluções de família explodidas nos membros ativos (ramo família).
pool_evolucoes as (
    select
        e.id_evolucao_sk,
        e.id_usuario_sk,
        e.id_familia,
        e.id_unidade_sk,
        e.id_unidade,
        e.data_evolucao,
        e.descricao_evolucao
    from {{ ref('fct_evolucoes') }} as e
    where
        e.origem_modulo = 'administrativa'
        and e.tipo_evolucao = 'F'
        and regexp_extract(e.descricao_evolucao, r'<h3>(.*?)</h3>') = 'CRAS - Ficha de Atendimento Individualizado'
    union all
    select
        f.id_evolucao_sk,
        u.id_usuario_sk,
        f.id_familia,
        f.id_unidade_sk,
        f.id_unidade,
        f.data_evolucao,
        f.descricao_evolucao
    from {{ ref('fct_evolucoes') }} as f
    inner join {{ ref('raw_membros_familia') }} as m on f.id_familia = m.id_familia
    inner join {{ ref('dim_usuarios') }} as u on m.id_paciente = u.id_usuario
    where f.origem_modulo = 'familia' and m.data_saida is null
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
    left join {{ ref('dim_usuarios') }} as u on p.id_usuario_sk = u.id_usuario_sk
),

-- extração via macro genérica (label/valor por campo do formulário)
campos_evolucoes as (
    select * from {{ extrair_campos_html_evolucao(
        source_relation = 'base_evolucoes',
        id_cols = ['id_evolucao_sk', 'id_usuario_sk', 'id_familia', 'id_unidade_sk', 'id_unidade', 'data_evolucao', 'nome_usuario']
    ) }}
),

-- classificação: pivô label -> coluna (uma linha por evolução)
encaminhamentos as (
    select
        id_evolucao_sk,
        id_usuario_sk,
        id_familia,
        id_unidade_sk,
        id_unidade,
        data_evolucao,
        nome_usuario,
        max(case when label like '%Encaminhamentos - %SMAS%' then nullif(valor, '') end) as encaminhamento_smas,
        max(case when label like '%Encaminhamentos - Benefícios%' then nullif(valor, '') end) as encaminhamento_beneficios,
        max(case when label like '%Encaminhamentos Órgãos%' then nullif(valor, '') end) as encaminhamento_orgaos
    from campos_evolucoes
    group by 1, 2, 3, 4, 5, 6, 7
),

-- agregação C2/C3 (grão família), C4 (grão indivíduo) e C5 (grão família)
agg_encaminhamentos as (
    select
        id_unidade_sk,
        {{ contar('coalesce(cast(id_familia as string), id_usuario_sk)', "regexp_contains(encaminhamento_beneficios, '(?i)Cadastro/Atualização Cadúnico')") }} as encaminhamento_cadunico_c2_c3,
        {{ contar('id_usuario_sk', "regexp_contains(encaminhamento_beneficios, '(?i)BPC - Idoso|BPC - PCD')") }} as encaminhamento_bpc_c4,
        {{ contar('coalesce(cast(id_familia as string), id_usuario_sk)', "regexp_contains(encaminhamento_orgaos, '(?i)CREAS')") }} as encaminhamento_creas_c5
    from encaminhamentos
    where
        {{ no_mes('data_evolucao') }}
        and (encaminhamento_beneficios is not null or encaminhamento_orgaos is not null)
        and (nome_usuario not like '%TESTES%' or nome_usuario is null)
    group by 1
),

-- ============================================================================
-- III. Bloco D — convivência e atividades coletivas
-- ============================================================================
paif_membros as (
    select distinct
        id_familia,
        id_usuario,
        id_unidade
    from paif
),

-- extração: presenças do mês + tipo de atividade + idade/deficiência
presencas as (
    select
        pr.id_usuario,
        a.id_tipo_atividade,
        {{ calc_idade('u.data_nascimento', 'fim_do_mes') }} as idade_anos,
        u.flag_deficiencia
    from {{ ref('fct_presencas_usuarios') }} as pr
    left join {{ ref('dim_atividades_grupo') }} as a on pr.id_atividade = a.id_atividade
    left join {{ ref('dim_usuarios') }} as u on pr.id_usuario = u.id_usuario
    where {{ no_mes('pr.data_presenca') }}
),

-- transformação: associa cada presença ao vínculo PAIF do usuário
coletivo_membros as (
    select
        p.id_unidade,
        p.id_familia,
        pr.id_usuario,
        pr.idade_anos,
        pr.id_tipo_atividade,
        pr.flag_deficiencia
    from presencas as pr
    inner join paif_membros as p on pr.id_usuario = p.id_usuario
),

-- D.2 a D.7
agg_coletivos as (
    select
        id_unidade,
        {{ contar('id_usuario', 'idade_anos between 0 and 6') }} as total_criancas_0_6_d2,
        {{ contar('id_usuario', 'idade_anos between 7 and 14 and id_tipo_atividade in (1, 2)') }} as total_criancas_7_14_scfv_d3,
        {{ contar('id_usuario', 'idade_anos between 15 and 17 and id_tipo_atividade = 3') }} as total_adolescentes_15_17_scfv_d4,
        {{ contar('id_usuario', 'idade_anos >= 60 and id_tipo_atividade = 4') }} as total_idosos_scfv_d5,
        {{ contar('id_usuario', 'id_tipo_atividade in (5, 6, 7, 8)') }} as total_participantes_nao_continuado_d6,
        {{ contar('id_usuario', "flag_deficiencia = 'S'") }} as total_pcd_coletivos_d7
    from coletivo_membros
    group by 1
),

-- D.1: famílias PAIF com membro presente em grupo no mês
agg_paif_coletivos as (
    select
        id_unidade,
        {{ contar('id_familia') }} as total_familias_grupos_paif_d1
    from coletivo_membros
    group by 1
),

-- ============================================================================
-- IV. Montagem final — 1 linha por unidade CRAS
-- ============================================================================
final as (
    select
        du.id_unidade_sk,
        du.id_unidade,
        du.nome_unidade,
        coalesce(tp.total_famil_paif_sistema_a1, 0) as total_famil_paif_sistema_a1,
        coalesce(tp.total_famil_paif_mes_atual_a2, 0) as total_famil_paif_mes_atual_a2,
        coalesce(ep.total_famil_paif_extrema_pobreza_b1, 0) as total_famil_paif_extrema_pobreza_b1,
        coalesce(bf.total_famil_paif_bf_b2, 0) as total_famil_paif_bf_b2,
        coalesce(bf.total_famil_paif_descumprimento_b3, 0) as total_famil_paif_descumprimento_b3,
        coalesce(bf.total_famil_paif_bpc_b4, 0) as total_famil_paif_bpc_b4,
        coalesce(bf.trab_infantil_crianca_adolescente_b5, 0) as trab_infantil_crianca_adolescente_b5,
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
    left join agg_paif as tp on du.id_unidade = tp.id_unidade
    left join agg_extrema_pobreza as ep on du.id_unidade = ep.id_unidade
    left join agg_beneficios_vulnerabilidades as bf on du.id_unidade = bf.id_unidade
    left join agg_acolhimento as ac on du.id_unidade = ac.id_unidade
    left join agg_atendimentos as atd on du.id_unidade_sk = atd.id_unidade_sk
    left join agg_encaminhamentos as ev on du.id_unidade_sk = ev.id_unidade_sk
    left join agg_atendimentos_domiciliar as adm on du.id_unidade_sk = adm.id_unidade_sk
    left join agg_paif_coletivos as pc on du.id_unidade = pc.id_unidade
    left join agg_coletivos as col on du.id_unidade = col.id_unidade
)

select * from final
