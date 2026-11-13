{{ config(alias='planilha_unificada', schema=var('pu_candidato_schema', 'pu_candidatos')) }}
-- noqa: disable=ST06
-- Contrato por assunto; identificadores públicos restritos aos campos autorizados.
with encaminhamentos_centro_pop as (
    select
        id_usuario,
        string_agg(
            valor, ', '
            order by if(tipo_registro = 'atendimento_social', 0, 1), concat(label, ': ', valor)
        ) as encaminhamentos
    from (
        select distinct
            r.id_usuario,
            r.tipo_registro,
            trim(e.label) as label,
            trim(e.valor) as valor
        from {{ ref('int_registros_centro_pop') }} as r, unnest(r.encaminhamentos) as e
        where
            r.tipo_registro in ('atendimento_social', 'pai')
            and nullif(trim(e.label), '') is not null
            and nullif(trim(e.valor), '') is not null
    )
    group by id_usuario
),

procedimentos_centro_pop as (
    select
        id_usuario,
        string_agg(
            valor, ', '
            order by if(tipo_registro = 'atendimento_social', 0, 1), valor
        ) as procedimentos
    from (
        select distinct
            r.id_usuario,
            r.tipo_registro,
            trim(demanda) as valor
        from {{ ref('int_registros_centro_pop') }} as r, unnest(r.demandas) as demanda
        where
            r.tipo_registro in ('atendimento_social', 'pai')
            and nullif(trim(demanda), '') is not null
    )
    group by id_usuario
),

cp_registros as (
    select
        p.id_usuario,
        array_agg((
            select as struct
                p.id_unidade,
                e.nome_unidade,
                coalesce(e.email_filtro, '') as email_filtro,
                p.centro_pop.*
        )
        order by p.id_unidade) as registros
    from {{ ref('int_perfil_centro_pop_por_unidade') }} as p
    left join {{ ref('int_filtro_email_unidades') }} as e on p.id_unidade = e.id_unidade
    group by p.id_usuario
),


historico_centro_pop as (
    select
        h.id_usuario,
        array_agg((
            select as struct
                h.* except (id_usuario),
                coalesce(e.email_filtro, '') as email_filtro
        ) order by h.mes_referencia desc, h.id_unidade asc) as historico_mensal
    from {{ ref('int_centro_pop_historico_mensal') }} as h
    left join {{ ref('int_filtro_email_unidades') }} as e on h.id_unidade = e.id_unidade
    group by h.id_usuario
),


projetos_por_usuario as (
    select
        u.id_usuario,
        array_agg(struct(
            coalesce({{ map_nome_projeto_social('ps.id_projeto_social') }}, format('Projeto não mapeado (ID %d)', ps.id_projeto_social)) as nome_projeto_social,
            ps.id_projeto_social, ps.id_unidade, un.nome_unidade, coalesce(em.email_filtro, '') as email_filtro, ps.indicador_ativo, ps.data_cadastro, ps.data_cancelamento
        ) order by ps.id_projeto_social, ps.id_unidade) as projetos_sociais
    from {{ ref('int_projetos_sociais_por_unidade') }} as ps
    inner join {{ ref('dim_usuarios') }} as u on ps.id_usuario = u.id_usuario
    left join {{ ref('dim_unidades') }} as un on ps.id_unidade = un.id_unidade
    left join {{ ref('int_filtro_email_unidades') }} as em on ps.id_unidade = em.id_unidade
    group by u.id_usuario
),

atendimentos_por_usuario as (
    select
        p.id_usuario,
        array_agg((
            select as struct
                a.*,
                coalesce(e.email_filtro, '') as email_assunto
        )
        order by a.data_atendimento, a.hora_atendimento, a.id_atendimento_modulo) as atendimentos
    from {{ ref('int_atendimentos_por_usuario') }} as p, unnest(p.atendimentos) as a
    left join {{ ref('int_filtro_email_unidades') }} as e on a.id_unidade = e.id_unidade
    group by p.id_usuario
),

formularios_por_evolucao as (
    select
        p.id_usuario,
        f.id_evolucao_modulo,
        array_agg(struct(f.titulo_formulario, f.observacoes, f.campos, f.data_ocorrencia, f.origem_data_ocorrencia, f.situacao_data_ocorrencia) order by posicao) as formularios
    from {{ ref('int_formularios_por_usuario') }} as p, unnest(p.formularios) as f with offset as posicao
    group by p.id_usuario, f.id_evolucao_modulo
),

registros_evolucoes as (
    select
        e.id_usuario,
        array_agg(struct(
            e.id_evolucao_modulo, e.origem_modulo, e.id_familia, e.id_unidade, e.nome_unidade,
            e.tipo_servico, coalesce(em.email_filtro, '') as email_filtro, e.data_evolucao, e.data_cancelamento, e.tipo_evolucao,
            e.nome_profissional, e.nome_atividade,
            {{ texto_html_legivel('e.descricao_evolucao') }} as descricao_evolucao,
            coalesce(f.formularios, []) as formularios
        ) order by e.data_evolucao, e.id_evolucao_modulo) as evolucoes
    from {{ ref('int_evolucoes_por_usuario') }} as e
    left join {{ ref('int_filtro_email_unidades') }} as em on e.id_unidade = em.id_unidade
    left join formularios_por_evolucao as f on e.id_usuario = f.id_usuario and e.id_evolucao_modulo = f.id_evolucao_modulo
    group by e.id_usuario
),

escopo_centro_pop as (
    select
        s.id_usuario,
        array_agg(if(s.tipo_registro = 'participacao', struct(
            coalesce(cast(s.id_unidade as string), '') as id_unidade,
            coalesce(cast(s.id_atividade as string), '') as id_atividade,
            coalesce(s.nome_atividade, '') as nome_atividade,
            coalesce(s.nome_unidade, '') as nome_unidade,
            coalesce(cast(s.data_presenca as string), '') as data_presenca,
            coalesce(s.hora_presenca, '') as hora_presenca
        ), null) ignore nulls order by s.data_presenca, s.hora_presenca, s.id_atividade, s.id_registro) as participacoes
    from {{ ref('int_registros_centro_pop') }} as s
    group by s.id_usuario
)

select
    struct(
        coalesce(cast(p.id_usuario as string), '') as id_usuario,
        coalesce(cast(p.nome as string), '') as nome,
        coalesce(cast(p.nome_social as string), '') as nome_social,
        coalesce(cast(p.filiacao_mae as string), '') as filiacao_mae,
        coalesce(cast(p.nacionalidade as string), '') as nacionalidade,
        coalesce(cast(p.condicao_estrangeira as string), '') as condicao_estrangeira,
        coalesce(cast(p.pais_origem as string), '') as pais_origem,
        coalesce(cast(p.cpf as string), '') as cpf,
        coalesce(cast(p.data_nascimento as string), '') as data_nascimento,
        coalesce(cast(p.sexo as string), '') as sexo,
        coalesce(cast(p.raca_cor as string), 'NÃO INFORMADO') as raca_cor,
        coalesce(cast(p.genero as string), '') as genero,
        coalesce(cast(p.bairro as string), 'NÃO INFORMADO') as bairro,
        coalesce(cast(p.prontuario as string), '') as prontuario,
        {{ pu_flag_sim_nao("coalesce(cast(p.flag_cadunico as string), '')") }} as flag_cadunico,
        coalesce(cast(p.pontuacao as string), '') as pontuacao,
        coalesce(cast(p.grau_dependencia as string), '') as grau_dependencia,
        coalesce(cast(p.nome_apoiador as string), '') as nome_apoiador,
        coalesce(cast(p.numero_processo_decisao_apoiada as string), '') as numero_processo_decisao_apoiada,
        coalesce(cast(p.renda_ativa as string), '') as renda_ativa,
        {{ pu_flag_sim_nao("coalesce(cast(p.atvd_remunerada as string), '')") }} as atvd_remunerada,
        {{ pu_flag_sim_nao("coalesce(cast(p.flag_decisao_apoiada as string), '')") }} as flag_decisao_apoiada,
        {{ pu_flag_sim_nao("coalesce(cast(p.flag_saude_mental_comprometida as string), '')") }} as flag_saude_mental_comprometida,
        coalesce(cast(p.motivo_acolhimento as string), '') as motivo_acolhimento,
        coalesce(cast(p.orientacao_sexual as string), '') as orientacao_sexual,
        coalesce(cast(p.vinculo_trabalhista as string), '') as vinculo_trabalhista,
        {{ pu_flag_sim_nao("coalesce(cast(p.flag_trabalha as string), '')") }} as flag_trabalha,
        coalesce(cast(p.profissao as string), '') as profissao,
        {{ pu_flag_sim_nao("coalesce(cast(p.flag_frequenta_escola as string), '')") }} as flag_frequenta_escola,
        coalesce(cast(p.escolaridade_indice as string), '') as escolaridade_indice,
        {{ pu_flag_sim_nao("coalesce(cast(p.flag_recebe_beneficio as string), '')") }} as flag_recebe_beneficio,
        {{ pu_flag_sim_nao("coalesce(cast(p.flag_curatela as string), '')") }} as flag_curatela,
        coalesce(cast(p.tipo_curatela as string), '') as tipo_curatela,
        {{ pu_flag_sim_nao("coalesce(cast(p.flag_deficiencia as string), '')") }} as flag_deficiencia,
        {{ pu_flag_sim_nao("coalesce(cast(p.flag_situacao_rua as string), '')") }} as flag_situacao_rua,
        coalesce(cast(p.origem_demanda as string), '') as origem_demanda,
        {{ pu_flag_sim_nao("coalesce(cast(p.flag_possui_violacao_direito as string), '')") }} as flag_possui_violacao_direito,
        coalesce(unidade_referencia.nome_unidade, '') as nome_unidade_referencia,
        coalesce(cast(p.uf_nascimento as string), '') as uf_nascimento,
        coalesce(trim(cast(p.cidade_nascimento as string)), '') as cidade_nascimento,
        coalesce(cast(p.serie_escolar as string), '') as serie_escolar,
        coalesce(cast(p.data_cadunico_informada as string), '') as data_cadunico_informada,
        coalesce(cast(cadastro.nis_cadastro as string), '') as nis,
        coalesce(cast(cadastro.numero_rg_cadastro as string), '') as numero_rg,
        if(nullif(trim(cast(cadastro.numero_rg_cadastro as string)), '') is not null, 'Sim', 'Não') as possui_rg,
        if(nullif(trim(cast(detalhes_cadastro.matricula_certidao_nascimento as string)), '') is not null, 'Sim', 'Não') as possui_certidao_nascimento,
        coalesce(cast(detalhes_cadastro.matricula_certidao_nascimento as string), '') as numero_certidao_nascimento,
        cast('' as string) as data_atualizacao,
        cast('' as string) as data_limite_cadastro_atual,
        cast('' as string) as flag_familia_ambigua,
        cast('' as string) as flag_nis_ambiguo,
        cast('' as string) as flag_rg_ambiguo,
        cast('' as string) as flag_certidao_ambigua,
        array(
            select as struct coalesce(cast(item1.descricao as string), '') as descricao
            from unnest(p.beneficio) as item1 with offset as posicao
            order by posicao
        ) as beneficio,
        array(
            select as struct coalesce(cast(item1.descricao as string), '') as descricao
            from unnest(p.deficiencia) as item1 with offset as posicao
            order by posicao
        ) as deficiencia,
        array(
            select as struct
                coalesce(cast(item1.descricao as string), '') as descricao,
                coalesce(cast(item1.origem as string), '') as origem
            from unnest(p.violacoes) as item1 with offset as posicao
            order by posicao
        ) as violacoes,
        coalesce(end_bi.endereco.logradouro, '') as logradouro,
        coalesce(end_bi.endereco.numero, '') as numero,
        coalesce(end_bi.endereco.complemento, '') as complemento,
        coalesce(end_bi.endereco.referencia_ou_comunidade, '') as referencia_ou_comunidade,
        case when safe_cast(p.data_nascimento as date) <= current_date('America/Sao_Paulo') then {{ calc_idade('safe_cast(p.data_nascimento as date)', "current_date('America/Sao_Paulo')") }} end as idade,
        coalesce(p.tipo_atividade_remunerada, '') as tipo_atividade_remunerada,
        if(nullif(trim(cast(p.cpf as string)), '') is not null, 'Sim', 'Não') as possui_cpf,
        {{ idade_faixa("case when safe_cast(p.data_nascimento as date) <= current_date('America/Sao_Paulo') then " ~ calc_idade('safe_cast(p.data_nascimento as date)', "current_date('America/Sao_Paulo')") ~ " end") }} as idade_faixa
    ) as usuario,
    array(
        select as struct
            coalesce(cast(item1.id_familia as string), '') as id_familia,
            coalesce(cast(item1.nome_responsavel as string), '') as nome_responsavel,
            {{ pu_flag_sim_nao("coalesce(cast(item1.flag_responsavel_atual as string), '')") }} as flag_responsavel_atual,
            {{ pu_flag_sim_nao("coalesce(cast(item1.flag_ativo as string), '')") }} as flag_ativo,
            coalesce(cast(item1.data_ultima_modificacao as string), '') as data_ultima_modificacao,
            array(
                select as struct
                    coalesce(cast(item2.id_usuario as string), '') as id_usuario,
                    coalesce(cast(item2.nome as string), '') as nome,
                    coalesce(cast(item2.parentesco_responsavel as string), '') as parentesco_responsavel
                from unnest(item1.membros) as item2 with offset as posicao
                order by posicao
            ) as membros,
            array(
                select as struct
                    coalesce(cast(item2.data_entrada as string), '') as data_entrada,
                    coalesce(cast(item2.data_saida as string), '') as data_saida,
                    coalesce(cast(item2.parentesco_responsavel as string), '') as parentesco_responsavel,
                    coalesce(cast(item2.motivo_saida as string), '') as motivo_saida
                from unnest(item1.vinculos) as item2 with offset as posicao
                order by posicao
            ) as vinculos,
            array(
                select as struct
                    coalesce(item2.nome_unidade, '') as nome_unidade,
                    coalesce(cast(item2.data_cadastro as string), '') as data_cadastro,
                    coalesce(cast(item2.data_cancelamento as string), '') as data_cancelamento,
                    coalesce(cast(item2.nome_servico as string), '') as nome_servico
                from unnest(item1.servicos) as item2 with offset as posicao
                order by posicao
            ) as servicos
        from unnest(f.familias) as item1 with offset as posicao
        order by posicao
    ) as familias,
    array(
        select as struct
            coalesce(cast(item1.nome_projeto_social as string), '') as nome_projeto_social,
            item1.id_projeto_social,
            item1.id_unidade,
            item1.email_filtro,
            coalesce(item1.nome_unidade, '') as nome_unidade,
            {{ pu_flag_sim_nao("coalesce(cast(item1.indicador_ativo as string), '')") }} as indicador_ativo,
            coalesce(cast(item1.data_cadastro as string), '') as data_cadastro,
            coalesce(cast(item1.data_cancelamento as string), '') as data_cancelamento
        from unnest(proj.projetos_sociais) as item1 with offset as posicao
        order by posicao
    ) as projetos_sociais,
    struct(
        {{ consolidar_emails('coalesce(a.atendimentos, [])', campo='email_assunto') }} as email_filtro,
        array_length(coalesce(a.atendimentos, [])) as quantidade,
        (select coalesce(sum(array_length(a.profissionais)), 0) from unnest(coalesce(a.atendimentos, [])) as a) as quantidade_linhas_bi,
        {{ pu_contagens_diretas('coalesce(a.atendimentos, [])') }},
        array(
            select as struct
                coalesce(cast(item1.id_atendimento_modulo as string), '') as id_atendimento_modulo,
                item1.email_assunto as email_filtro,
                coalesce(item1.id_familia, '') as id_familia,
                coalesce(cast(item1.origem_modulo as string), '') as origem_modulo,
                coalesce(cast(item1.id_unidade as string), '') as id_unidade,
                coalesce(item1.nome_unidade, '') as nome_unidade,
                coalesce(cast(item1.tipo_atendimento_descricao as string), '') as tipo_atendimento_descricao,
                coalesce(cast(item1.data_atendimento as string), '') as data_atendimento_informada,
                {{ normalizar_hora_atendimento('item1.hora_atendimento') }} as hora_atendimento,
                {{ pu_flag_sim_nao("coalesce(cast(item1.flag_cancelado as string), '')") }} as flag_cancelado,
                array(
                    select as struct
                        coalesce(cast(item2.nome_profissional as string), '') as nome_profissional,
                        array(
                            select coalesce(item3, '') from unnest(item2.descricoes_cbo) as item3 with offset as posicao
                            order by posicao
                        ) as descricoes_cbo,
                        coalesce(cast(item2.cbo_principal_descricao as string), '') as cbo_principal_descricao,
                        coalesce(cast(item2.id_profissional as string), '') as id_profissional,
                        coalesce(item2.profissional_cbo, '') as profissional_cbo,
                        coalesce(item2.classificacao_atendimento, '') as classificacao_atendimento,
                        coalesce(item2.nome_atendimento, '') as nome_atendimento,
                        coalesce(cast(item2.id_familia_referencia as string), '') as id_familia_referencia,
                        coalesce(item2.id_atendimento_legado, '') as id_atendimento_legado
                    from unnest(item1.profissionais) as item2 with offset as posicao
                    order by posicao
                ) as profissionais,
                coalesce(item1.tipo_servico, '') as tipo_servico,
                coalesce(item1.classificacao_atendimento, '') as classificacao_atendimento,
                coalesce(item1.projetos_sociais_familia, '') as projetos_sociais_familia,
                coalesce(item1.cas, '') as cas,
                coalesce(item1.email_unidade, '') as email_unidade,
                coalesce(item1.email_cas, '') as email_cas,
                coalesce(item1.cadastrante, '') as cadastrante,
                coalesce(cast(item1.data_cadastro_atendimento as string), '') as data_atendimento_sistema,
                coalesce(item1.tipo_unidade, '') as tipo_unidade,
                item1.elegivel_indicador
            from unnest(coalesce(a.atendimentos, [])) as item1 with offset as posicao
            order by posicao
        ) as registros
    ) as atendimentos,
    struct(
        {{ consolidar_emails('coalesce(e.evolucoes, [])', campo='email_filtro') }} as email_filtro,
        array_length(coalesce(e.evolucoes, [])) as quantidade,
        {{ pu_contagens_diretas('coalesce(e.evolucoes, [])') }},
        array(
            select as struct
                coalesce(cast(item1.id_evolucao_modulo as string), '') as id_evolucao_modulo,
                item1.email_filtro,
                coalesce(cast(item1.origem_modulo as string), '') as origem_modulo,
                coalesce(cast(item1.id_familia as string), '') as id_familia,
                coalesce(cast(item1.id_unidade as string), '') as id_unidade,
                coalesce(item1.nome_unidade, '') as nome_unidade,
                coalesce(cast(item1.data_evolucao as string), '') as data_evolucao,
                coalesce(cast(item1.data_cancelamento as string), '') as data_cancelamento,
                coalesce(cast(item1.tipo_evolucao as string), '') as tipo_evolucao,
                coalesce(cast(item1.nome_profissional as string), '') as nome_profissional,
                coalesce(cast(item1.nome_atividade as string), '') as nome_atividade,
                coalesce(cast(item1.descricao_evolucao as string), '') as descricao_evolucao,
                array(
                    select as struct
                        coalesce(cast(item2.titulo_formulario as string), '') as titulo_formulario,
                        coalesce(cast(item2.data_ocorrencia as string), '') as data_ocorrencia,
                        item2.origem_data_ocorrencia,
                        item2.situacao_data_ocorrencia,
                        coalesce(cast(item2.observacoes as string), '') as observacoes,
                        array(
                            select concat(coalesce(cast(item3.label as string), ''), ': ', coalesce(cast(item3.valor as string), ''))
                            from unnest(item2.campos) as item3 with offset as posicao
                            order by posicao
                        ) as campos
                    from unnest(item1.formularios) as item2 with offset as posicao
                    order by posicao
                ) as formularios,
                coalesce(item1.tipo_servico, '') as tipo_servico
            from unnest(coalesce(e.evolucoes, [])) as item1 with offset as posicao
            order by posicao
        ) as registros
    ) as evolucoes,
    struct(
        {{ pu_cpop_perfil_publico('perfil_cp', listas_em_texto=true, indicadores_medicos={
            'uso_substancias_psicoativas': "case upper(trim(cast(medico.flag_uso_substancias_psicoativas as string))) when 'S' then 'Sim' when 'N' then 'Não' else '' end",
            'possui_problema_saude': "case upper(trim(cast(medico.flag_possui_comorbidade as string))) when 'S' then 'Sim' when 'N' then 'Não' else '' end"
        }) }},
        {{ consolidar_emails('coalesce(cp_reg.registros, [])', campo='email_filtro') }} as email_filtro,
        coalesce(cp_reg.registros, []) as registros,
        coalesce(hist.historico_mensal, []) as historico_mensal,
        esc.participacoes[safe_offset(array_length(esc.participacoes) - 1)].nome_atividade as ultima_participacao_atividade,
        esc.participacoes[safe_offset(array_length(esc.participacoes) - 1)].nome_unidade as ultima_participacao_unidade,
        esc.participacoes[safe_offset(array_length(esc.participacoes) - 1)].data_presenca as ultima_participacao_data,
        esc.participacoes[safe_offset(array_length(esc.participacoes) - 1)].hora_presenca as ultima_participacao_hora,
        coalesce(cp_enc.encaminhamentos, '') as encaminhamentos,
        coalesce(cp_proc.procedimentos, '') as procedimentos
    ) as centro_pop,
    struct(
        {{ consolidar_emails('coalesce(cr_reg.registros, [])', campo='email_filtro') }} as email_filtro,
        coalesce(cr_reg.registros, []) as registros,
        perfil_creas_simples.mse_data_ocorrencia,
        perfil_creas_simples.mse_data_registro,
        perfil_creas_simples.mse_id_unidade,
        perfil_creas_simples.mse_nome_unidade,
        perfil_creas_simples.mse_observacoes,
        perfil_creas_simples.mse_com_que_frequencia_vai_a_casa_da_familia_ou_pessoa_de_referencia,
        perfil_creas_simples.mse_com_quem_moraria,
        perfil_creas_simples.mse_1o_bimestre_documentacao,
        perfil_creas_simples.mse_1o_bimestre_beneficios,
        perfil_creas_simples.mse_1o_bimestre_cadunico,
        perfil_creas_simples.mse_1o_bimestre_localizacao,
        perfil_creas_simples.mse_1o_bimestre_outras_acoes,
        perfil_creas_simples.mse_1o_bimestre_saude,
        perfil_creas_simples.mse_adolescente_em_reinfracao,
        perfil_creas_simples.mse_adolescente_tem_historico_de_cumprimento_de_mse_em_meio_fechado_ou_semiliberdade,
        perfil_creas_simples.mse_avaliacao_tecnica_bimestral_1o_bimestre,
        perfil_creas_simples.mse_cras_de_referencia,
        perfil_creas_simples.mse_creas_de_referencia,
        perfil_creas_simples.mse_carga_horaria,
        perfil_creas_simples.mse_com_quem_residia_anteriormente,
        perfil_creas_simples.mse_data,
        perfil_creas_simples.mse_data_de_inclusao_no_mse,
        perfil_creas_simples.mse_desdobramento,
        perfil_creas_simples.mse_em_caso_de_cumprimento_de_mse_qual_o_status,
        perfil_creas_simples.mse_em_caso_de_cumprimento_de_psc_qual_o_local,
        perfil_creas_simples.mse_endereco_cras,
        perfil_creas_simples.mse_endereco_creas,
        perfil_creas_simples.mse_especialidade,
        perfil_creas_simples.mse_gostaria_de_retornar_para_casa_ou_convivio_anterior,
        perfil_creas_simples.mse_gostaria_de_retornar_para_casa_ou_convivio_anterior_com_quem_moraria,
        perfil_creas_simples.mse_local,
        perfil_creas_simples.mse_mantem_contato_com_a_familia_ou_pessoa_de_referencia,
        perfil_creas_simples.mse_mantem_contato_com_a_familia_ou_pessoa_de_referencia_com_que_frequencia_vai_a_casa_da_familia_ou_pessoa_de_referencia,
        perfil_creas_simples.mse_medida_aplicada,
        perfil_creas_simples.mse_nome_do_local_de_cumprimento_de_psc,
        perfil_creas_simples.mse_outras_atividades,
        perfil_creas_simples.mse_por_que_se_afastou_da_familia_ou_pessoa_de_referencia,
        perfil_creas_simples.mse_procedimento,
        perfil_creas_simples.mse_profissional_cras,
        perfil_creas_simples.mse_profissional_creas,
        perfil_creas_simples.mse_quais_atividades_gostaria_de_participar,
        perfil_creas_simples.mse_registrar_o_que_conseguiu_cumprir_e_justificar_o_que_nao_conseguiu_cumprir_no_prazo_estabelecido,
        perfil_creas_simples.mse_servico_de_protecao_social_ao_adolescente_em_cumprimento_de_la_ou_psc,
        perfil_creas_simples.mse_telefone_cras,
        perfil_creas_simples.mse_telefone_creas,
        perfil_creas_simples.mse_tempo_de_cumprimento,
        perfil_creas_simples.mse_tipo,
        perfil_creas_simples.mse_tipo_de_ato_infracional,
        perfil_creas_simples.mse_usuario_interditado,
        perfil_creas_simples.mse_retorno_data_ocorrencia,
        perfil_creas_simples.mse_retorno_data_registro,
        perfil_creas_simples.mse_retorno_id_unidade,
        perfil_creas_simples.mse_retorno_nome_unidade,
        perfil_creas_simples.mse_retorno_observacoes,
        perfil_creas_simples.mse_retorno_adolescente_em_reinfracao,
        perfil_creas_simples.mse_retorno_data_do_desligamento,
        perfil_creas_simples.mse_retorno_desligamento,
        perfil_creas_simples.mse_retorno_em_caso_de_cumprimento_de_mse_qual_o_status,
        perfil_creas_simples.mse_retorno_em_caso_de_cumprimento_de_psc_qual_o_local,
        perfil_creas_simples.mse_retorno_motivo_do_desligamento,
        perfil_creas_simples.mse_retorno_nome_do_local_de_cumprimento_de_psc,
        perfil_creas_simples.peti_data_ocorrencia,
        perfil_creas_simples.peti_data_registro,
        perfil_creas_simples.peti_id_unidade,
        perfil_creas_simples.peti_nome_unidade,
        perfil_creas_simples.peti_observacoes,
        perfil_creas_simples.peti_data_da_inclusao_no_peti,
        perfil_creas_simples.peti_demanda_inicial,
        perfil_creas_simples.peti_forma_de_acesso_informe_a_origem_de_acesso,
        perfil_creas_simples.peti_marcacao_nos_campo_cadunico,
        perfil_creas_simples.peti_orientacoes_e_encaminhamentos,
        perfil_creas_simples.peti_outras_demandas_vulnerabilidades,
        perfil_creas_simples.peti_outras_formas_de_acesso,
        perfil_creas_simples.peti_outros,
        perfil_creas_simples.peti_tipo_de_trabalho_infantil,
        perfil_creas_simples.peti_e_acompanhado_em_outro_creas,
        perfil_creas_simples.peti_e_necessario_realizar_a_gestao_de_beneficios,
        perfil_creas_simples.scfv_data_ocorrencia,
        perfil_creas_simples.scfv_data_registro,
        perfil_creas_simples.scfv_id_unidade,
        perfil_creas_simples.scfv_nome_unidade,
        perfil_creas_simples.scfv_observacoes,
        perfil_creas_simples.scfv_data_da_solicitacao_do_acolhimento,
        perfil_creas_simples.scfv_demanda_de_acolhimento_institucional_por_decisao_judicial,
        perfil_creas_simples.scfv_nome_do_cras_responsavel,
        perfil_creas_simples.scfv_o_usuario_participa_do_scfv_servico_de_convivencia_e_fortalecimento_de_vinculos,
        perfil_creas_simples.scfv_oriundo_de_instituicao_de_acolhimento_interditado,
        perfil_creas_simples.scfv_usuario_interditado,
        perfil_creas_simples.scfv_orgao_de_origem_do_acolhimento,
        perfil_creas_simples.peti_desligamento_data_ocorrencia,
        perfil_creas_simples.peti_desligamento_data_registro,
        perfil_creas_simples.peti_desligamento_id_unidade,
        perfil_creas_simples.peti_desligamento_nome_unidade,
        perfil_creas_simples.peti_desligamento_observacoes,
        perfil_creas_simples.peti_desligamento_data_do_desligamento_do_peti,
        perfil_creas_simples.peti_desligamento_motivo_do_desligamento,
        perfil_creas_simples.peti_desligamento_outros,
        perfil_creas_simples.passo_a_passo_ultima_atividade,
        perfil_creas_simples.passo_a_passo_ultima_data,
        perfil_creas_simples.passo_a_passo_ultima_hora,
        perfil_creas_simples.passo_a_passo_ultima_unidade
    ) as creas,
    struct(
        {{ consolidar_emails('coalesce(perfil_alta.registros, [])', campo='email_filtro') }} as email_filtro,
        coalesce(perfil_alta.registros, []) as registros,
        perfil_alta.ultimo_ciclo.id_ciclo,
        perfil_alta.ultimo_ciclo.id_unidade,
        perfil_alta.ultimo_ciclo.nome_unidade,
        perfil_alta.ultimo_ciclo.data_entrada,
        perfil_alta.ultimo_ciclo.data_saida,
        perfil_alta.ultimo_ciclo.motivo_saida,
        perfil_alta.ultimo_ciclo.tempo_permanencia_dias
    ) as alta_complexidade,
    {{ consolidar_unidades("coalesce(a.atendimentos, [])") }} as nome_unidade,
    {{ consolidar_emails("coalesce(a.atendimentos, [])") }} as email_filtro
from {{ ref('dim_usuarios') }} as p
left join projetos_por_usuario as proj on p.id_usuario = proj.id_usuario
left join {{ ref('int_usuarios_enderecos') }} as end_bi on p.id_usuario = end_bi.id_usuario
left join {{ ref('int_usuarios_familias') }} as f on p.id_usuario = f.id_usuario
left join atendimentos_por_usuario as a on p.id_usuario = a.id_usuario
left join registros_evolucoes as e on p.id_usuario = e.id_usuario
left join procedimentos_centro_pop as cp_proc on p.id_usuario = cp_proc.id_usuario
left join encaminhamentos_centro_pop as cp_enc on p.id_usuario = cp_enc.id_usuario
left join cp_registros as cp_reg on cast(p.id_usuario as string) = cp_reg.id_usuario
left join {{ ref('creas') }} as cr_reg on p.id_usuario = cr_reg.id_usuario
left join historico_centro_pop as hist on cast(p.id_usuario as string) = hist.id_usuario
left join escopo_centro_pop as esc on p.id_usuario = esc.id_usuario
left join {{ ref('centro_pop') }} as perfil_cp on p.id_usuario = perfil_cp.id_usuario
left join {{ ref('dim_unidades') }} as unidade_referencia on safe_cast(p.id_unidade_referencia as int64) = unidade_referencia.id_unidade

left join {{ ref('int_perfil_creas_simples') }} as perfil_creas_simples on p.id_usuario = perfil_creas_simples.id_usuario
left join {{ ref('alta_complexidade') }} as perfil_alta on p.id_usuario = perfil_alta.id_usuario

left join {{ ref('raw_usuarios') }} as cadastro on p.id_usuario = cadastro.id_paciente
left join {{ ref('raw_usuarios_detalhes') }} as detalhes_cadastro on p.id_usuario = detalhes_cadastro.id_paciente
left join {{ ref('raw_usuarios_informacoes_medicas') }} as medico on p.id_usuario = medico.id_usuario
