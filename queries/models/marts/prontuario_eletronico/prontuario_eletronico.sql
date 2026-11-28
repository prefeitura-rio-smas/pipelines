{% set origens_vinculos = [
    'atendimentos_por_usuario',
    'registros_evolucoes',
    'projetos_por_usuario',
    'escopo_centro_pop',
    'historico_centro_pop',
    'cp_registros',
    'creas_por_unidade',
    'acolhimentos_por_unidade',
] %}
-- noqa: disable=ST06
-- Contrato por assunto; identificadores públicos restritos aos campos autorizados.
with familias_por_usuario as (
    with membros as (
        select distinct
            m.id_paciente as id_usuario,
            m.id_familia,
            m.id_membro,
            m.id_familia_nova,
            m.id_motivo_saida,
            m.data_entrada,
            m.data_saida,
            m.parentesco_responsavel,
            ms.descricao_motivo_saida as motivo_saida
        from {{ ref('raw_membros_familia') }} as m
        left join {{ ref('raw_familias_motivos_saida') }} as ms on m.id_motivo_saida = ms.id_motivo_saida
        where m.id_paciente is not null and m.id_familia is not null
    ),

    vinculos as (
        select
            id_usuario,
            id_familia,
            array_agg(struct(
                id_membro,
                id_familia_nova,
                id_motivo_saida,
                data_entrada,
                data_saida,
                parentesco_responsavel,
                motivo_saida
            ) order by data_entrada, id_membro, data_saida, id_familia_nova, id_motivo_saida, parentesco_responsavel) as vinculos
        from membros
        group by id_usuario, id_familia
    ),

    familias_core as (
        select distinct
            id_familia,
            id_usuario_responsavel,
            nome_responsavel,
            flag_ativo,
            data_ultima_modificacao
        from {{ ref('int_familias') }}
    ),

    familias as (
        select * from familias_core
        union all
        select distinct
            r.id_familia,
            r.id_usuario_responsavel,
            cast(null as string) as nome_responsavel,
            r.flag_ativo,
            r.data_ultima_modificacao
        from {{ ref('raw_familias') }} as r
        -- O core exige responsável cadastrado. Preservar famílias fora dessa cobertura.
        where not exists (
            select 1
            from familias_core as c
            where c.id_familia = r.id_familia
        )
    ),

    -- Inclui responsáveis sem registro na tabela de membros.
    usuarios_familias as (
        select
            id_usuario,
            id_familia
        from membros
        union distinct
        select
            id_usuario_responsavel as id_usuario,
            id_familia
        from familias
        where id_usuario_responsavel is not null and id_familia is not null
    ),

    membros_atuais_candidatos as (
        select
            id_familia,
            id_usuario,
            parentesco_responsavel,
            data_entrada,
            id_membro,
            0 as prioridade_responsavel
        from membros
        where data_saida is null
        union all
        select
            id_familia,
            id_usuario_responsavel as id_usuario,
            'Responsável' as parentesco_responsavel,
            null as data_entrada,
            null as id_membro,
            1 as prioridade_responsavel
        from familias
        where id_usuario_responsavel is not null and id_familia is not null
    ),

    membros_atuais_unicos as (
        select
            id_familia,
            id_usuario,
            parentesco_responsavel
        from membros_atuais_candidatos
        qualify row_number() over (
            partition by id_familia, id_usuario
            order by prioridade_responsavel desc, data_entrada desc, id_membro desc, parentesco_responsavel asc
        ) = 1
    ),

    composicao_familiar as (
        select
            m.id_familia,
            array_agg(struct(
                m.id_usuario,
                u.nome,
                m.parentesco_responsavel
            ) order by coalesce(u.nome, ''), m.id_usuario) as membros
        from membros_atuais_unicos as m
        left join {{ ref('dim_usuarios') }} as u on m.id_usuario = u.id_usuario
        group by m.id_familia
    ),

    servicos as (
        select
            s.id_familia,
            array_agg(struct(
                h.id_servico_assistencial,
                h.id_unidade,
                u.nome_unidade,
                h.id_profissional as id_login_cadastro,
                h.data_cadastro,
                h.data_cancelamento,
                h.nome_servico_socioassistencial as nome_servico
            ) order by h.data_cadastro, h.id_servico_assistencial, h.id_unidade, h.data_cancelamento, h.id_profissional) as servicos
        from {{ ref('int_servicos_agregados') }} as s,
            unnest(s.servicos_historico) as h
        left join {{ ref('dim_unidades') }} as u on h.id_unidade = u.id_unidade
        group by s.id_familia
    )

    select
        uf.id_usuario,
        array_agg(struct(
            uf.id_familia,
            f.id_usuario_responsavel,
            f.nome_responsavel,
            f.id_usuario_responsavel = uf.id_usuario as flag_responsavel_atual,
            f.flag_ativo,
            f.data_ultima_modificacao,
            coalesce(cf.membros, []) as membros,
            coalesce(v.vinculos, []) as vinculos,
            coalesce(s.servicos, []) as servicos
        ) order by uf.id_familia) as familias
    from usuarios_familias as uf
    left join familias as f on uf.id_familia = f.id_familia
    left join composicao_familiar as cf on uf.id_familia = cf.id_familia
    left join vinculos as v on uf.id_usuario = v.id_usuario and uf.id_familia = v.id_familia
    left join servicos as s on uf.id_familia = s.id_familia
    group by uf.id_usuario
),

encaminhamentos_centro_pop as (
    select
        id_usuario,
        id_unidade,
        string_agg(
            valor, ', '
            order by if(tipo_registro = 'atendimento_social', 0, 1), concat(label, ': ', valor)
        ) as encaminhamentos
    from (
        select distinct
            r.id_usuario,
            r.id_unidade,
            r.tipo_registro,
            trim(e.label) as label,
            trim(e.valor) as valor
        from {{ ref('int_registros_centro_pop') }} as r, unnest(r.encaminhamentos) as e
        where
            r.tipo_registro in ('atendimento_social', 'pai')
            and nullif(trim(e.label), '') is not null
            and nullif(trim(e.valor), '') is not null
    )
    group by id_usuario, id_unidade
),

ultimo_atendimento_tecnico_centro_pop as (
    select
        id_usuario,
        id_unidade,
        max(data_ultimo_atend_tecnico_informada) as data_ultimo_atend_tecnico_informada
    from ({{ datas_atendimentos_tecnicos_centro_pop() }})
    group by id_usuario, id_unidade
),

procedimentos_centro_pop as (
    select
        id_usuario,
        id_unidade,
        string_agg(
            valor, ', '
            order by if(tipo_registro = 'atendimento_social', 0, 1), valor
        ) as procedimentos
    from (
        select distinct
            r.id_usuario,
            r.id_unidade,
            r.tipo_registro,
            trim(demanda) as valor
        from {{ ref('int_registros_centro_pop') }} as r, unnest(r.demandas) as demanda
        where
            r.tipo_registro in ('atendimento_social', 'pai')
            and nullif(trim(demanda), '') is not null
    )
    group by id_usuario, id_unidade
),

cp_registros as (
    select
        p.id_usuario,
        u.id_unidade,
        u.perfil,
        [(
            select as struct
                u.id_unidade,
                e.nome_unidade,
                {{ campos_perfil_centro_pop('u.centro_pop') }}
        )] as registros
    from {{ ref('int_perfil_centro_pop') }} as p
    cross join unnest(p.registros) as u
    left join {{ ref('int_filtro_email_unidades') }} as e on u.id_unidade = e.id_unidade
),

historico_centro_pop as (
    select
        h.id_usuario,
        h.id_unidade,
        array_agg(
            (
                select as struct
                    {{ campos_historico_centro_pop('h') }}
            )
            order by h.mes_referencia desc, h.id_unidade asc
        ) as historico_mensal
    from {{ ref('int_resumo_centro_pop') }} as h
    group by h.id_usuario, h.id_unidade
),


projetos_por_usuario as (
    select
        u.id_usuario,
        ps.id_unidade,
        array_agg(struct(
            coalesce({{ map_nome_projeto_social('ps.id_projeto_social') }}, format('Projeto não mapeado (ID %d)', ps.id_projeto_social)) as nome_projeto_social,
            ps.id_projeto_social, ps.id_unidade, un.nome_unidade, ps.indicador_ativo, ps.data_cadastro, ps.data_cancelamento
        ) order by ps.id_projeto_social, ps.id_unidade) as projetos_sociais
    from (
        {{ projetos_dos_membros() }}
    ) as ps
    inner join {{ ref('dim_usuarios') }} as u on ps.id_usuario = u.id_usuario
    left join {{ ref('dim_unidades') }} as un on ps.id_unidade = un.id_unidade
    group by u.id_usuario, ps.id_unidade
),

atendimentos_por_usuario as (
    select
        p.id_usuario,
        a.id_unidade,
        array_agg(
            (select as struct a.* replace (
                [profissional] as profissionais,
                profissional.classificacao_atendimento as classificacao_atendimento
            ))
            order by pos_evento, pos_profissional
        ) as atendimentos,
        array_agg(distinct nullif(trim(cast(profissional.id_familia_referencia as string)), '') ignore nulls) as ids_familias_referenciadas
    from (
        {{ agrupar_atendimentos() }}
    ) as p
    cross join unnest(p.atendimentos) as a with offset as pos_evento
    cross join unnest(a.profissionais) as profissional with offset as pos_profissional
    group by p.id_usuario, a.id_unidade
),

formularios_por_evolucao as (
    select
        p.id_usuario,
        f.id_evolucao_modulo,
        array_agg(struct(f.titulo_formulario, f.observacoes, f.campos, f.data_ocorrencia, f.origem_data_ocorrencia, f.situacao_data_ocorrencia) order by posicao) as formularios
    from ({{ agrupar_formularios(ref('int_formularios')) }}) as p, unnest(p.formularios) as f with offset as posicao
    group by p.id_usuario, f.id_evolucao_modulo
),

registros_evolucoes as (
    select
        e.id_usuario,
        e.id_unidade,
        array_agg(struct(
            e.id_evolucao_modulo, e.origem_modulo, e.id_familia, e.id_unidade, e.nome_unidade,
            e.tipo_servico, e.data_evolucao, e.data_cancelamento, e.tipo_evolucao,
            e.nome_profissional, e.nome_atividade,
            {{ texto_html_legivel('e.descricao_evolucao') }} as descricao_evolucao,
            coalesce(f.formularios, []) as formularios
        ) order by e.data_evolucao, e.id_evolucao_modulo) as evolucoes
    from (
        {{ preparar_evolucoes() }}
    ) as e
    left join formularios_por_evolucao as f on e.id_usuario = f.id_usuario and e.id_evolucao_modulo = f.id_evolucao_modulo
    group by e.id_usuario, e.id_unidade
),

escopo_centro_pop as (
    select
        s.id_usuario,
        s.id_unidade,
        array_agg(if(s.tipo_registro = 'participacao', struct(
            coalesce(cast(s.id_unidade as string), '') as id_unidade,
            coalesce(cast(s.id_atividade as string), '') as id_atividade,
            coalesce(s.nome_atividade, '') as nome_atividade,
            coalesce(s.nome_unidade, '') as nome_unidade,
            coalesce(cast(s.data_presenca as string), '') as data_presenca,
            coalesce(s.hora_presenca, '') as hora_presenca
        ), null) ignore nulls order by s.data_presenca, s.hora_presenca, s.id_atividade, s.id_registro) as participacoes
    from {{ ref('int_registros_centro_pop') }} as s
    group by s.id_usuario, s.id_unidade
),

perfis_creas as (
    select
        p.id_usuario,
        u.id_unidade,
        u.creas
    from {{ ref('int_perfil_creas') }} as p
    cross join unnest(p.registros) as u
),

creas_por_unidade as (
    select
        p.id_usuario,
        p.id_unidade,
        p.creas,
        [(
            select as struct
                p.id_unidade,
                em.nome_unidade,
                {{ campos_perfil_creas('p.creas') }}
        )] as registros
    from perfis_creas as p
    left join {{ ref('int_filtro_email_unidades') }} as em on p.id_unidade = em.id_unidade
),

acolhimentos_por_unidade as (
    select
        p.id_usuario,
        h.id_unidade,
        array_agg(h order by h.data_entrada, h.id_ciclo) as registros,
        array_agg(
            h order by
                if(h.data_saida is null, 0, 1), h.data_entrada desc nulls last,
                h.id_ciclo desc, to_json_string(h) desc
            limit 1
        )[offset(0)] as ultimo_ciclo
    from {{ ref('alta_complexidade') }} as p, unnest(p.historico) as h
    group by p.id_usuario, h.id_unidade
),

unidades_por_usuario as (
    -- Uma chave por vínculo; os eventos continuam nas coleções de cada assunto.
    {% for origem in origens_vinculos %}
        select
            cast(id_usuario as string) as id_usuario,
            id_unidade
        from {{ origem }}
        {% if not loop.last %}union distinct{% endif %}
    {% endfor %}
)

select
    concat(
        coalesce(cast(p.id_usuario as string), ''),
        ':',
        coalesce(cast(vinculo.id_unidade as string), '')
    ) as id_usuario_unidade,
    coalesce(cast(p.id_usuario as string), '') as id_usuario,
    coalesce(cast(vinculo.id_unidade as string), '') as id_unidade,
    coalesce(unidade.nome_unidade, '') as nome_unidade,
    coalesce(permissoes.email_filtro, '') as email_filtro,
    struct(
        coalesce(cast(p.id_usuario as string), '') as id_usuario,
        coalesce(cast(p.nome as string), '') as nome,
        coalesce(cast(p.nome_social as string), '') as nome_social,
        coalesce(cast(p.filiacao_mae as string), '') as filiacao_mae,

        coalesce(cast(p.data_nascimento as string), '') as data_nascimento,
        case when safe_cast(p.data_nascimento as date) <= current_date('America/Sao_Paulo') then {{ calc_idade('safe_cast(p.data_nascimento as date)', "current_date('America/Sao_Paulo')") }} end as idade,
        {{ idade_faixa("case when safe_cast(p.data_nascimento as date) <= current_date('America/Sao_Paulo') then " ~ calc_idade('safe_cast(p.data_nascimento as date)', "current_date('America/Sao_Paulo')") ~ " end") }} as idade_faixa,
        coalesce(cast(p.uf_nascimento as string), '') as uf_nascimento,
        coalesce(trim(cast(p.cidade_nascimento as string)), '') as cidade_nascimento,

        coalesce(cast(p.nacionalidade as string), '') as nacionalidade,
        coalesce(cast(p.pais_origem as string), '') as pais_origem,
        coalesce(cast(p.condicao_estrangeira as string), '') as condicao_estrangeira,

        coalesce(cast(p.sexo as string), '') as sexo,
        coalesce(cast(p.genero as string), '') as genero,
        coalesce(cast(p.orientacao_sexual as string), '') as orientacao_sexual,
        coalesce(cast(p.raca_cor as string), 'NÃO INFORMADO') as raca_cor,

        if(nullif(trim(cast(p.cpf as string)), '') is not null, 'Sim', 'Não') as possui_cpf,
        coalesce(cast(p.cpf as string), '') as cpf,
        if(nullif(trim(cast(cadastro.numero_rg_cadastro as string)), '') is not null, 'Sim', 'Não') as possui_rg,
        coalesce(cast(cadastro.numero_rg_cadastro as string), '') as numero_rg,
        cast('' as string) as flag_rg_ambiguo,
        if(nullif(trim(cast(detalhes_cadastro.matricula_certidao_nascimento as string)), '') is not null, 'Sim', 'Não') as possui_certidao_nascimento,
        coalesce(cast(detalhes_cadastro.matricula_certidao_nascimento as string), '') as numero_certidao_nascimento,
        cast('' as string) as flag_certidao_ambigua,

        {{ pu_flag_sim_nao("coalesce(cast(p.flag_cadunico as string), '')") }} as flag_cadunico,
        coalesce(cast(p.data_cadunico_informada as string), '') as data_cadunico_informada,
        coalesce(cast(cadastro.nis_cadastro as string), '') as nis,
        cast('' as string) as flag_nis_ambiguo,

        coalesce(cast(p.bairro as string), 'NÃO INFORMADO') as bairro,
        coalesce(end_bi.endereco.logradouro, '') as logradouro,
        coalesce(end_bi.endereco.numero, '') as numero,
        coalesce(end_bi.endereco.complemento, '') as complemento,
        coalesce(end_bi.endereco.referencia_ou_comunidade, '') as referencia_ou_comunidade,

        coalesce(cast(p.escolaridade_indice as string), '') as escolaridade_indice,
        {{ pu_flag_sim_nao("coalesce(cast(p.flag_frequenta_escola as string), '')") }} as flag_frequenta_escola,
        coalesce(cast(p.serie_escolar as string), '') as serie_escolar,

        {{ pu_flag_sim_nao("coalesce(cast(p.flag_trabalha as string), '')") }} as flag_trabalha,
        coalesce(cast(p.profissao as string), '') as profissao,
        coalesce(cast(p.vinculo_trabalhista as string), '') as vinculo_trabalhista,
        coalesce(p.tipo_atividade_remunerada, '') as tipo_atividade_remunerada,
        {{ pu_flag_sim_nao("coalesce(cast(p.atvd_remunerada as string), '')") }} as atvd_remunerada,
        coalesce(cast(p.renda_ativa as string), '') as renda_ativa,

        {{ pu_flag_sim_nao("coalesce(cast(p.flag_recebe_beneficio as string), '')") }} as flag_recebe_beneficio,
        array(
            select as struct coalesce(cast(item1.descricao as string), '') as descricao
            from unnest(p.beneficio) as item1 with offset as posicao
            order by posicao
        ) as beneficio,

        {{ pu_flag_sim_nao("coalesce(cast(p.flag_decisao_apoiada as string), '')") }} as flag_decisao_apoiada,
        coalesce(cast(p.numero_processo_decisao_apoiada as string), '') as numero_processo_decisao_apoiada,
        coalesce(cast(p.nome_apoiador as string), '') as nome_apoiador,
        {{ pu_flag_sim_nao("coalesce(cast(p.flag_saude_mental_comprometida as string), '')") }} as flag_saude_mental_comprometida,
        {{ pu_flag_sim_nao("coalesce(cast(p.flag_curatela as string), '')") }} as flag_curatela,
        coalesce(cast(p.tipo_curatela as string), '') as tipo_curatela,

        {{ pu_flag_sim_nao("coalesce(cast(p.flag_deficiencia as string), '')") }} as flag_deficiencia,
        array(
            select as struct coalesce(cast(item1.descricao as string), '') as descricao
            from unnest(p.deficiencia) as item1 with offset as posicao
            order by posicao
        ) as deficiencia,
        coalesce(cast(p.grau_dependencia as string), '') as grau_dependencia,

        {{ pu_flag_sim_nao("coalesce(cast(p.flag_situacao_rua as string), '')") }} as flag_situacao_rua,
        coalesce(cast(p.origem_demanda as string), '') as origem_demanda,
        coalesce(cast(p.motivo_acolhimento as string), '') as motivo_acolhimento,
        coalesce(unidade_referencia.nome_unidade, '') as nome_unidade_referencia,
        {{ pu_flag_sim_nao("coalesce(cast(p.flag_possui_violacao_direito as string), '')") }} as flag_possui_violacao_direito,
        array(
            select as struct
                coalesce(cast(item1.descricao as string), '') as descricao,
                coalesce(cast(item1.origem as string), '') as origem
            from unnest(p.violacoes) as item1 with offset as posicao
            order by posicao
        ) as violacoes,

        cast('' as string) as data_atualizacao,
        cast('' as string) as data_limite_cadastro_atual,
        cast('' as string) as flag_familia_ambigua,
        coalesce(cast(p.pontuacao as string), '') as pontuacao
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
                    coalesce(cast(item2.parentesco_responsavel as string), '') as parentesco_responsavel,
                    coalesce(cast(item2.data_entrada as string), '') as data_entrada,
                    coalesce(cast(item2.data_saida as string), '') as data_saida,
                    coalesce(cast(item2.motivo_saida as string), '') as motivo_saida
                from unnest(item1.vinculos) as item2 with offset as posicao
                order by posicao
            ) as vinculos,
            {{ servicos_da_familia_na_unidade('item1.servicos', 'vinculo.id_unidade') }} as servicos
        from unnest(f.familias) as item1 with offset as posicao
        where trim(cast(item1.id_familia as string)) in unnest(coalesce(a.ids_familias_referenciadas, []))
        order by posicao
    ) as familias,
    array(
        select as struct
            item1.id_projeto_social,
            coalesce(cast(item1.nome_projeto_social as string), '') as nome_projeto_social,
            item1.id_unidade,
            coalesce(item1.nome_unidade, '') as nome_unidade,
            {{ pu_flag_sim_nao("coalesce(cast(item1.indicador_ativo as string), '')") }} as indicador_ativo,
            coalesce(cast(item1.data_cadastro as string), '') as data_cadastro,
            coalesce(cast(item1.data_cancelamento as string), '') as data_cancelamento
        from unnest(proj.projetos_sociais) as item1 with offset as posicao
        order by posicao
    ) as projetos_sociais,
    struct(
        array_length(coalesce(a.atendimentos, [])) as quantidade,
        {{ pu_contagens_diretas('coalesce(a.atendimentos, [])') }},
        array(
            select as struct
                coalesce(cast(item1.id_atendimento_modulo as string), '') as id_atendimento_modulo,
                coalesce(item1.id_familia, '') as id_familia,
                coalesce(cast(item1.origem_modulo as string), '') as origem_modulo,
                coalesce(cast(item1.id_unidade as string), '') as id_unidade,
                coalesce(item1.nome_unidade, '') as nome_unidade,
                coalesce(item1.tipo_unidade, '') as tipo_unidade,
                coalesce(item1.cas, '') as cas,
                coalesce(cast(item1.data_atendimento as string), '') as data_atendimento_informada,
                {{ normalizar_hora_atendimento('item1.hora_atendimento') }} as hora_atendimento,
                coalesce(cast(item1.tipo_atendimento_descricao as string), '') as tipo_atendimento_descricao,
                coalesce(item1.tipo_servico, '') as tipo_servico,
                coalesce(item1.classificacao_atendimento, '') as classificacao_atendimento,
                coalesce(item1.projetos_sociais_familia, '') as projetos_sociais_familia,
                item1.elegivel_indicador,
                item1.atendimento_compartilhado,
                array(
                    select as struct
                        nullif(trim(upper(item1.cadastrante)), '') as id_profissional,
                        coalesce(cast(item2.nome_profissional as string), '') as nome_profissional,
                        coalesce(cast(item2.id_familia_referencia as string), '') as id_familia_referencia,
                        coalesce(item2.id_atendimento_legado, '') as id_atendimento_legado,
                        array(
                            select coalesce(item3, '') from unnest(item2.descricoes_cbo) as item3 with offset as posicao
                            order by posicao
                        ) as descricoes_cbo,
                        coalesce(cast(item2.cbo_principal_descricao as string), '') as cbo_principal_descricao,
                        coalesce(item2.profissional_cbo, '') as profissional_cbo,
                        coalesce(item2.classificacao_atendimento, '') as classificacao_atendimento,
                        coalesce(item2.nome_atendimento, '') as nome_atendimento
                    from unnest(item1.profissionais) as item2 with offset as posicao
                    order by posicao
                ) as profissionais,
                coalesce(item1.cadastrante, '') as cadastrante,
                coalesce(cast(item1.data_cadastro_atendimento as string), '') as data_atendimento_sistema,
                coalesce(item1.email_unidade, '') as email_unidade,
                coalesce(item1.email_cas, '') as email_cas
            from unnest(coalesce(a.atendimentos, [])) as item1 with offset as posicao
            order by posicao
        ) as registros
    ) as atendimentos,
    struct(
        array_length(coalesce(e.evolucoes, [])) as quantidade,
        {{ pu_contagens_diretas('coalesce(e.evolucoes, [])') }},
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
                coalesce(item1.tipo_servico, '') as tipo_servico,
                coalesce(cast(item1.nome_atividade as string), '') as nome_atividade,
                coalesce(cast(item1.descricao_evolucao as string), '') as descricao_evolucao,
                coalesce(cast(item1.nome_profissional as string), '') as nome_profissional,
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
                ) as formularios
            from unnest(coalesce(e.evolucoes, [])) as item1 with offset as posicao
            order by posicao
        ) as registros
    ) as evolucoes,
    (
        select as struct
            cp.unidades_atendimento,
            cp.possui_registro_pai,
            cp.data_ocorrencia_pai,
            cp.origem_data_ocorrencia_pai,
            cp.situacao_data_ocorrencia_pai,
            cp.data_registro_pai,
            cp.data_resposta_pai,
            cp.data_inclusao_pai,
            cp.data_inclusao_pai_informada,
            cp.flag_incluido_acompanhamento_pai,
            cp.flag_empregabilidade_imediata,
            cp.capacidade_habilidades,
            cp.flag_interesse_curso,
            cp.demandas_pai,
            cp.encaminhamentos_pai,
            cp.observacoes_pai,
            cp.data_ocorrencia_atendimento_social,
            cp.origem_data_ocorrencia_atendimento_social,
            cp.situacao_data_ocorrencia_atendimento_social,
            cp.data_registro_atendimento_social,
            cp.data_resposta_atendimento_social,
            cp.demandas_atendimento_social,
            cp.encaminhamentos_atendimento_social,
            cp.observacoes_atendimento_social,
            cp.encaminhamentos,
            cp.procedimentos,
            cp.data_ultimo_atend_tecnico_informada,
            cp.data_ocorrencia_saude,
            cp.origem_data_ocorrencia_saude,
            cp.situacao_data_ocorrencia_saude,
            cp.data_registro_saude,
            cp.data_resposta_saude,
            cp.uso_substancias_psicoativas,
            cp.situacao_saude,
            cp.local_tratamento,
            cp.possui_problema_saude,
            cp.realiza_acompanhamento_saude,
            cp.observacoes_saude,
            cp.data_ocorrencia_situacao_rua,
            cp.origem_data_ocorrencia_situacao_rua,
            cp.situacao_data_ocorrencia_situacao_rua,
            cp.data_registro_situacao_rua,
            cp.data_resposta_situacao_rua,
            cp.motivo_principal_permanencia_rua,
            cp.motivo_secundario_permanencia_rua,
            cp.possui_referencias_familiares,
            cp.territorio_referencia_familia,
            cp.possibilidade_reinsercao_familiar,
            cp.data_ocorrencia_desligamento_pai,
            cp.origem_data_ocorrencia_desligamento_pai,
            cp.situacao_data_ocorrencia_desligamento_pai,
            cp.data_hora_registro_desligamento_pai,
            cp.data_hora_resposta_desligamento_pai,
            cp.data_desligamento_pai,
            cp.data_desligamento_pai_informada,
            cp.motivo_desligamento_pai,
            cp.complemento_motivo_desligamento_pai,
            cp.observacoes_desligamento_pai,
            cp.ultima_participacao_atividade,
            cp.ultima_participacao_unidade,
            cp.ultima_participacao_data,
            cp.ultima_participacao_hora,
            cp.registros,
            cp.historico_mensal
        from unnest([struct(
            {{ perfil_publico_centro_pop('cp_reg.perfil', listas_em_texto=true, indicadores_medicos={
                'uso_substancias_psicoativas': "case when cp_reg.id_usuario is null then '' else case upper(trim(cast(medico.flag_uso_substancias_psicoativas as string))) when 'S' then 'Sim' when 'N' then 'Não' else '' end end",
                'possui_problema_saude': "case when cp_reg.id_usuario is null then '' else case upper(trim(cast(medico.flag_possui_comorbidade as string))) when 'S' then 'Sim' when 'N' then 'Não' else '' end end"
            }) }},
            coalesce(cp_reg.registros, []) as registros,
            coalesce(hist.historico_mensal, []) as historico_mensal,
            esc.participacoes[safe_offset(array_length(esc.participacoes) - 1)].nome_atividade as ultima_participacao_atividade,
            esc.participacoes[safe_offset(array_length(esc.participacoes) - 1)].nome_unidade as ultima_participacao_unidade,
            esc.participacoes[safe_offset(array_length(esc.participacoes) - 1)].data_presenca as ultima_participacao_data,
            esc.participacoes[safe_offset(array_length(esc.participacoes) - 1)].hora_presenca as ultima_participacao_hora,
            coalesce(cp_enc.encaminhamentos, '') as encaminhamentos,
            coalesce(cp_proc.procedimentos, '') as procedimentos,
            coalesce(cast(ultimo_cp.data_ultimo_atend_tecnico_informada as string), '') as data_ultimo_atend_tecnico_informada
        )]) as cp
    ) as centro_pop,
    struct(
        {{ campos_perfil_creas('cr_reg.creas') | trim | indent(8) }},
        coalesce(cr_reg.registros, []) as registros
    ) as creas,
    struct(
        perfil_alta.ultimo_ciclo.id_ciclo,
        perfil_alta.ultimo_ciclo.id_unidade,
        perfil_alta.ultimo_ciclo.nome_unidade,
        perfil_alta.ultimo_ciclo.data_entrada,
        perfil_alta.ultimo_ciclo.data_saida,
        perfil_alta.ultimo_ciclo.motivo_saida,
        perfil_alta.ultimo_ciclo.tempo_permanencia_dias,
        coalesce(perfil_alta.registros, []) as registros
    ) as alta_complexidade
from {{ ref('dim_usuarios') }} as p
left join unidades_por_usuario as vinculo on cast(p.id_usuario as string) = vinculo.id_usuario
left join {{ ref('dim_unidades') }} as unidade on vinculo.id_unidade = unidade.id_unidade
left join {{ ref('int_filtro_email_unidades') }} as permissoes on vinculo.id_unidade = permissoes.id_unidade
left join projetos_por_usuario as proj
    on
        p.id_usuario = proj.id_usuario
        and vinculo.id_unidade is not distinct from proj.id_unidade
left join {{ ref('int_usuarios_enderecos') }} as end_bi on p.id_usuario = end_bi.id_usuario
left join familias_por_usuario as f on p.id_usuario = f.id_usuario
left join atendimentos_por_usuario as a
    on
        p.id_usuario = a.id_usuario
        and vinculo.id_unidade is not distinct from a.id_unidade
left join registros_evolucoes as e
    on
        p.id_usuario = e.id_usuario
        and vinculo.id_unidade is not distinct from e.id_unidade
left join procedimentos_centro_pop as cp_proc
    on
        p.id_usuario = cp_proc.id_usuario
        and vinculo.id_unidade is not distinct from cp_proc.id_unidade
left join encaminhamentos_centro_pop as cp_enc
    on
        p.id_usuario = cp_enc.id_usuario
        and vinculo.id_unidade is not distinct from cp_enc.id_unidade
left join cp_registros as cp_reg
    on
        cast(p.id_usuario as string) = cp_reg.id_usuario
        and vinculo.id_unidade is not distinct from cp_reg.id_unidade
left join creas_por_unidade as cr_reg
    on
        cast(p.id_usuario as string) = cr_reg.id_usuario
        and vinculo.id_unidade is not distinct from cr_reg.id_unidade
left join historico_centro_pop as hist
    on
        cast(p.id_usuario as string) = hist.id_usuario
        and vinculo.id_unidade is not distinct from hist.id_unidade
left join escopo_centro_pop as esc
    on
        p.id_usuario = esc.id_usuario
        and vinculo.id_unidade is not distinct from esc.id_unidade
left join {{ ref('dim_unidades') }} as unidade_referencia on safe_cast(p.id_unidade_referencia as int64) = unidade_referencia.id_unidade

left join acolhimentos_por_unidade as perfil_alta
    on
        p.id_usuario = perfil_alta.id_usuario
        and vinculo.id_unidade is not distinct from perfil_alta.id_unidade
left join ultimo_atendimento_tecnico_centro_pop as ultimo_cp
    on
        p.id_usuario = ultimo_cp.id_usuario
        and vinculo.id_unidade is not distinct from ultimo_cp.id_unidade

left join {{ ref('raw_usuarios') }} as cadastro on p.id_usuario = cadastro.id_paciente
left join {{ ref('raw_usuarios_detalhes') }} as detalhes_cadastro on p.id_usuario = detalhes_cadastro.id_paciente
left join {{ ref('raw_usuarios_informacoes_medicas') }} as medico on p.id_usuario = medico.id_usuario
