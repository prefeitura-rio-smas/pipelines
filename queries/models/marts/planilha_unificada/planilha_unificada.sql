{{ config(schema=var('pu_candidato_schema', 'pu_candidatos')) }}
-- noqa: disable=ST06
-- Contrato por assunto; identificadores públicos restritos aos campos autorizados.
with unidades as (
    select
        id_unidade,
        nome_unidade
    from {{ pu_ref('dim_unidades') }}
),

projetos_por_usuario as (
    select
        u.id_usuario,
        array_agg(struct(
            coalesce({{ map_nome_projeto_social('ps.id_projeto_social') }}, error(concat('Confirmar nome do projeto social: ', cast(ps.id_projeto_social as string)))) as nome_projeto_social,
            ps.id_unidade, un.nome_unidade, ps.indicador_ativo, ps.data_cadastro, ps.data_cancelamento
        ) order by posicao) as projetos_sociais
    from {{ pu_ref('dim_usuarios') }} as u,
        unnest(u.projetos_sociais) as ps with offset as posicao
    left join unidades as un on ps.id_unidade = un.id_unidade
    group by u.id_usuario
),

atendimentos_por_usuario as (
    select
        d.id_usuario,
        array_agg(
            (
                select as struct
                    a.* except (profissionais),
                    c.cas,
                    c.email_unidade,
                    c.email_cas,
                    c.email,
                    c.cadastrante,
                    c.data_cadastro_atendimento,
                    c.profissionais as complementos_profissionais,
                    a.profissionais
            ) order by
                a.data_atendimento, a.hora_atendimento, a.id_atendimento_modulo,
                a.id_unidade, a.id_tipo_atendimento, a.tipo_atendimento_descricao, a.flag_cancelado
        ) as atendimentos
    from {{ pu_ref('int_pu_atendimentos') }} as d, unnest(d.atendimentos) as a
    left join {{ pu_ref('int_pu_complementos_bi') }} as c on a.id_atendimento_modulo = c.id_atendimento_modulo and d.id_usuario = c.id_usuario
    group by d.id_usuario
),

formularios_por_evolucao as (
    select
        p.id_usuario,
        f.id_evolucao_modulo,
        array_agg(struct(f.titulo_formulario, f.observacoes, f.campos) order by posicao) as formularios
    from {{ pu_ref('int_pu_formularios') }} as p, unnest(p.formularios) as f with offset as posicao
    group by p.id_usuario, f.id_evolucao_modulo
),

registros_evolucoes as (
    select
        e.id_usuario,
        array_agg(struct(
            e.id_evolucao_modulo, e.origem_modulo, e.id_familia, e.id_unidade, e.nome_unidade,
            e.tipo_servico, e.data_evolucao, e.data_cancelamento, e.tipo_evolucao,
            e.nome_profissional, e.nome_atividade,
            {{ texto_html_legivel('e.descricao_evolucao') }} as descricao_evolucao,
            coalesce(f.formularios, []) as formularios
        ) order by e.data_evolucao, e.id_evolucao_modulo) as evolucoes
    from {{ pu_ref('int_pu_evolucoes') }} as e
    left join formularios_por_evolucao as f on e.id_usuario = f.id_usuario and e.id_evolucao_modulo = f.id_evolucao_modulo
    group by e.id_usuario
),

escopo_centro_pop as (
    select
        s.id_usuario,
        array_agg(if(s.tipo_registro != 'participacao', struct({{ pu_cpop_historico_publico('s') }}), null) ignore nulls order by s.data_registro, s.data_resposta, s.id_registro) as historico,
        array_agg(if(s.tipo_registro = 'participacao', struct(
            coalesce(cast(s.id_unidade as string), '') as id_unidade,
            coalesce(cast(s.id_atividade as string), '') as id_atividade,
            coalesce(s.nome_atividade, '') as nome_atividade,
            coalesce(s.nome_unidade, '') as nome_unidade,
            coalesce(cast(s.data_presenca as string), '') as data_presenca,
            coalesce(s.hora_presenca, '') as hora_presenca
        ), null) ignore nulls order by s.data_presenca, s.hora_presenca, s.id_atividade, s.id_registro) as participacoes
    from {{ pu_ref('int_pu_registros_centro_pop') }} as s
    group by s.id_usuario
),

base as (
    -- Contrato de apresentação: as chaves técnicas continuam nos intermediários.
    -- Não aplicar DISTINCT depois de retirar IDs: homônimos e descrições iguais
    -- não representam necessariamente o mesmo registro.
    select
        u.id_usuario,
        u.nome,
        u.nome_social,
        u.filiacao_mae,
        u.nacionalidade,
        u.condicao_estrangeira,
        u.pais_origem,
        u.cpf,
        u.data_nascimento,
        u.sexo,
        u.raca_cor,
        u.genero,
        u.bairro,
        u.prontuario,
        u.flag_cadunico,
        u.pontuacao,
        u.grau_dependencia,
        u.nome_apoiador,
        u.numero_processo_decisao_apoiada,
        u.renda_ativa,
        u.atvd_remunerada,
        u.flag_decisao_apoiada,
        u.flag_saude_mental_comprometida,
        u.motivo_acolhimento,
        u.orientacao_sexual,
        u.vinculo_trabalhista,
        u.flag_trabalha,
        u.profissao,
        u.flag_frequenta_escola,
        u.escolaridade_indice,
        u.flag_recebe_beneficio,
        u.flag_curatela,
        u.tipo_curatela,
        u.flag_deficiencia,
        u.flag_situacao_rua,
        u.origem_demanda,
        u.flag_possui_violacao_direito,
        u.id_unidade_referencia,
        array(
            select as struct
                v.descricao,
                v.origem
            from unnest(u.violacoes) as v with offset as posicao
            order by posicao
        ) as violacoes,
        array(
            select as struct d.descricao
            from unnest(u.deficiencia) as d with offset as posicao
            order by posicao
        ) as deficiencia,
        array(
            select as struct b.descricao
            from unnest(u.beneficio) as b with offset as posicao
            order by posicao
        ) as beneficio,
        coalesce(proj.projetos_sociais, []) as projetos_sociais,
        c.cadastro_complementar,
        end_bi.endereco,
        if(d.documentos_cadunico is null, null, (
            select as struct d.documentos_cadunico.* except (cpf_normalizado, id_familia_cadunico)
        )) as documentos_cadunico,
        array(
            select as struct
                f.* except (id_usuario_responsavel, vinculos, servicos),
                array(
                    select as struct v.* except (id_membro, id_familia_nova, id_motivo_saida)
                    from unnest(f.vinculos) as v with offset as posicao
                    order by posicao
                ) as vinculos,
                array(
                    select as struct
                        s.id_unidade,
                        s.nome_unidade,
                        s.data_cadastro,
                        s.data_cancelamento,
                        s.nome_servico
                    from unnest(f.servicos) as s with offset as posicao
                    order by posicao
                ) as servicos
            from unnest(f.familias) as f with offset as posicao
            order by posicao
        ) as familias,
        array(
            select as struct
                a.* except (id_atendimento, id_tipo_atendimento, profissionais, complementos_profissionais),
                {{ pu_cpop_classificacao('a') }} as classificacao_atendimento,
                array(
                    select as struct
                        pr.* except (codigos_cbo, cbo_principal_codigo),
                        cp.id_familia_referencia,
                        cp.id_atendimento_legado
                    from unnest(a.profissionais) as pr with offset as posicao
                    left join unnest(a.complementos_profissionais) as cp on pr.id_profissional = cp.id_profissional
                    order by posicao
                ) as profissionais
            from unnest(a.atendimentos) as a with offset as posicao
            order by posicao
        ) as atendimentos,
        coalesce(e.evolucoes, []) as evolucoes
    from {{ pu_ref('dim_usuarios') }} as u
    left join projetos_por_usuario as proj on u.id_usuario = proj.id_usuario
    left join {{ pu_ref('int_pu_cadastro') }} as c on u.id_usuario = c.id_usuario
    left join {{ pu_ref('int_pu_enderecos_bi') }} as end_bi on u.id_usuario = end_bi.id_usuario
    left join
        {{ pu_ref('int_pu_documentos_cadunico') }} as d
        on u.id_usuario = d.id_usuario
    left join {{ pu_ref('int_pu_familias') }} as f on u.id_usuario = f.id_usuario
    left join
        atendimentos_por_usuario as a
        on u.id_usuario = a.id_usuario
    left join
        registros_evolucoes as e
        on u.id_usuario = e.id_usuario
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
        coalesce(cast(p.raca_cor as string), '') as raca_cor,
        coalesce(cast(p.genero as string), '') as genero,
        coalesce(cast(p.bairro as string), '') as bairro,
        coalesce(cast(p.prontuario as string), '') as prontuario,
        coalesce(cast(p.flag_cadunico as string), '') as flag_cadunico,
        coalesce(cast(p.pontuacao as string), '') as pontuacao,
        coalesce(cast(p.grau_dependencia as string), '') as grau_dependencia,
        coalesce(cast(p.nome_apoiador as string), '') as nome_apoiador,
        coalesce(cast(p.numero_processo_decisao_apoiada as string), '') as numero_processo_decisao_apoiada,
        coalesce(cast(p.renda_ativa as string), '') as renda_ativa,
        coalesce(cast(p.atvd_remunerada as string), '') as atvd_remunerada,
        coalesce(cast(p.flag_decisao_apoiada as string), '') as flag_decisao_apoiada,
        coalesce(cast(p.flag_saude_mental_comprometida as string), '') as flag_saude_mental_comprometida,
        coalesce(cast(p.motivo_acolhimento as string), '') as motivo_acolhimento,
        coalesce(cast(p.orientacao_sexual as string), '') as orientacao_sexual,
        coalesce(cast(p.vinculo_trabalhista as string), '') as vinculo_trabalhista,
        coalesce(cast(p.flag_trabalha as string), '') as flag_trabalha,
        coalesce(cast(p.profissao as string), '') as profissao,
        coalesce(cast(p.flag_frequenta_escola as string), '') as flag_frequenta_escola,
        coalesce(cast(p.escolaridade_indice as string), '') as escolaridade_indice,
        coalesce(cast(p.flag_recebe_beneficio as string), '') as flag_recebe_beneficio,
        coalesce(cast(p.flag_curatela as string), '') as flag_curatela,
        coalesce(cast(p.tipo_curatela as string), '') as tipo_curatela,
        coalesce(cast(p.flag_deficiencia as string), '') as flag_deficiencia,
        coalesce(cast(p.flag_situacao_rua as string), '') as flag_situacao_rua,
        coalesce(cast(p.origem_demanda as string), '') as origem_demanda,
        coalesce(cast(p.flag_possui_violacao_direito as string), '') as flag_possui_violacao_direito,
        coalesce(unidade_referencia.nome_unidade, '') as nome_unidade_referencia,
        coalesce(cast(p.cadastro_complementar.uf_nascimento as string), '') as uf_nascimento,
        coalesce(cast(p.cadastro_complementar.serie_escolar as string), '') as serie_escolar,
        coalesce(cast(p.cadastro_complementar.data_cadunico_informada as string), '') as data_cadunico_informada,
        coalesce(cast(p.documentos_cadunico.nis as string), '') as nis,
        coalesce(cast(p.documentos_cadunico.numero_rg as string), '') as numero_rg,
        coalesce(cast(p.documentos_cadunico.possui_rg as string), '') as possui_rg,
        coalesce(cast(p.documentos_cadunico.possui_certidao_nascimento as string), '') as possui_certidao_nascimento,
        coalesce(cast(p.documentos_cadunico.numero_certidao_nascimento as string), '') as numero_certidao_nascimento,
        coalesce(cast(p.documentos_cadunico.data_atualizacao as string), '') as data_atualizacao,
        coalesce(cast(p.documentos_cadunico.data_limite_cadastro_atual as string), '') as data_limite_cadastro_atual,
        coalesce(cast(p.documentos_cadunico.flag_familia_ambigua as string), '') as flag_familia_ambigua,
        coalesce(cast(p.documentos_cadunico.flag_nis_ambiguo as string), '') as flag_nis_ambiguo,
        coalesce(cast(p.documentos_cadunico.flag_rg_ambiguo as string), '') as flag_rg_ambiguo,
        coalesce(cast(p.documentos_cadunico.flag_certidao_ambigua as string), '') as flag_certidao_ambigua,
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
        coalesce(p.endereco.logradouro, '') as logradouro,
        coalesce(p.endereco.numero, '') as numero,
        coalesce(p.endereco.complemento, '') as complemento,
        coalesce(p.endereco.referencia_ou_comunidade, '') as referencia_ou_comunidade
    ) as usuario,
    array(
        select as struct
            coalesce(cast(item1.id_familia as string), '') as id_familia,
            coalesce(cast(item1.nome_responsavel as string), '') as nome_responsavel,
            coalesce(cast(item1.flag_responsavel_atual as string), '') as flag_responsavel_atual,
            coalesce(cast(item1.flag_ativo as string), '') as flag_ativo,
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
        from unnest(p.familias) as item1 with offset as posicao
        order by posicao
    ) as familias,
    array(
        select as struct
            coalesce(cast(item1.nome_projeto_social as string), '') as nome_projeto_social,
            coalesce(item1.nome_unidade, '') as nome_unidade,
            coalesce(cast(item1.indicador_ativo as string), '') as indicador_ativo,
            coalesce(cast(item1.data_cadastro as string), '') as data_cadastro,
            coalesce(cast(item1.data_cancelamento as string), '') as data_cancelamento
        from unnest(p.projetos_sociais) as item1 with offset as posicao
        order by posicao
    ) as projetos_sociais,
    struct(
        array_length(p.atendimentos) as quantidade,
        {{ pu_contagens_diretas('p.atendimentos') }},
        array(
            select as struct
                coalesce(cast(item1.id_atendimento_modulo as string), '') as id_atendimento_modulo,
                coalesce(cast(item1.origem_modulo as string), '') as origem_modulo,
                coalesce(cast(item1.id_unidade as string), '') as id_unidade,
                coalesce(item1.nome_unidade, '') as nome_unidade,
                coalesce(cast(item1.tipo_atendimento_descricao as string), '') as tipo_atendimento_descricao,
                coalesce(cast(item1.data_atendimento as string), '') as data_atendimento,
                {{ normalizar_hora_atendimento('item1.hora_atendimento') }} as hora_atendimento,
                coalesce(cast(item1.flag_cancelado as string), '') as flag_cancelado,
                array(
                    select as struct
                        coalesce(cast(item2.nome_profissional as string), '') as nome_profissional,
                        array(
                            select coalesce(item3, '') from unnest(item2.descricoes_cbo) as item3 with offset as posicao
                            order by posicao
                        ) as descricoes_cbo,
                        coalesce(cast(item2.cbo_principal_descricao as string), '') as cbo_principal_descricao,
                        coalesce(cast(item2.id_profissional as string), '') as id_profissional,
                        coalesce(cast(item2.id_familia_referencia as string), '') as id_familia_referencia,
                        coalesce(item2.id_atendimento_legado, '') as id_atendimento_legado
                    from unnest(item1.profissionais) as item2 with offset as posicao
                    order by posicao
                ) as profissionais,
                coalesce(item1.tipo_servico, '') as tipo_servico,
                coalesce(item1.classificacao_atendimento, '') as classificacao_atendimento,
                coalesce(item1.cas, '') as cas,
                coalesce(item1.email_unidade, '') as email_unidade,
                coalesce(item1.email_cas, '') as email_cas,
                coalesce(item1.email, '') as email,
                coalesce(item1.cadastrante, '') as cadastrante,
                coalesce(cast(item1.data_cadastro_atendimento as string), '') as data_cadastro_atendimento
            from unnest(p.atendimentos) as item1 with offset as posicao
            order by posicao
        ) as registros
    ) as atendimentos,
    struct(
        array_length(p.evolucoes) as quantidade,
        {{ pu_contagens_diretas('p.evolucoes') }},
        array(
            select as struct
                coalesce(cast(item1.id_evolucao_modulo as string), '') as id_evolucao_modulo,
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
            from unnest(p.evolucoes) as item1 with offset as posicao
            order by posicao
        ) as registros
    ) as evolucoes,
    struct(struct(coalesce(esc.historico, []) as historico, coalesce(esc.participacoes, []) as participacoes, {{ pu_cpop_perfil_publico('perfil_cp') }}) as centro_pop) as escopos
from base as p
left join escopo_centro_pop as esc on p.id_usuario = esc.id_usuario
left join {{ pu_ref('dim_centro_pop') }} as perfil_cp on p.id_usuario = perfil_cp.id_usuario
left join unidades as unidade_referencia on safe_cast(p.id_unidade_referencia as int64) = unidade_referencia.id_unidade
