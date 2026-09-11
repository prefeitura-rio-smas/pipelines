-- Mart: Planilha Unificada Centro POP
-- Granularidade: 1 linha por usuário × mês de referência × Centro POP.
-- Base: usuários com pelo menos um atendimento (não cancelado) em unidade
-- Centro POP no mês de referência (coluna mes_referencia = primeiro dia do mês).
--
-- Fontes:
--   - dim_usuarios / raw_usuarios / raw_usuarios_saude_mental: perfil cadastral;
--   - fct_atendimentos + dim_profissionais: atendimentos e profissional de referência;
--   - fct_evolucoes (codigo_abrangencia = 29) + macro extrair_formulario:
--     formulários 'Centro POP - Plano de Atendimento Individual (PAI)',
--     'Centro POP - Atendimento Social' e 'Centro POP - Desligamento PAI';
--   - fct_evolucoes (codigo_abrangencia = 1, form 'Acolhimento'): motivos de
--     permanência na rua (não existe o campo 'Motivo da ida às ruas' na fonte);
--   - fct_evolucoes (codigo_abrangencia = 24, form 'Documentação Civil'):
--     registro de demandas de documentação;
--   - cadunico (raw_documento_pessoa + raw_identificacao_membro via CPF): NIS;
--   - fct_presencas_usuarios: oficinas/atividades coletivas no mês.

with centro_pop as (
    select
        id_unidade,
        nome_unidade
    from {{ ref('dim_unidades') }}
    where tipo_unidade = 'Centro POP'
),

-- Atendimentos do mês nas unidades Centro POP
atendimentos as (
    select
        a.id_usuario,
        a.id_unidade,
        a.id_atendimento,
        a.data_atendimento,
        a.tipo_atendimento_descricao as nome_atendimento,
        dp.nome as profissional,
        dp.cbo_principal_descricao as profissional_cbo,
        case
            -- Profissional de nível superior = grupo CBO 2 (descrições da fonte têm sufixo " CENTRO POP")
            when substr(trim(cast(dp.cbo_principal_codigo as string)), 1, 1) = '2' then 'Atendimento Técnico'
            when a.tipo_atendimento_descricao like '%Recepção%' then 'Atendimento Recepção'
            when dp.nome = 'ATENDIMENTO RECEPÇÃO' then 'Atendimento Recepção'
            when
                dp.cbo_principal_descricao in ('Administrador', 'Articulador Comunitário', 'Assistente administrativo', 'Educador social', 'Orientador social', 'Recepcionista')
                and a.tipo_atendimento_descricao like '%CadÚnico%'
                then 'Atendimento Recepção'
            else 'Outros Atendimentos'
        end as tipo_atendimento
    from {{ ref('fct_atendimentos') }} as a
    inner join {{ ref('dim_usuarios') }} as du on a.id_usuario_sk = du.id_usuario_sk
    inner join centro_pop as c on a.id_unidade = c.id_unidade
    left join {{ ref('dim_profissionais') }} as dp on a.id_profissional_sk = dp.id_profissional_sk
    where (a.flag_cancelado is null or a.flag_cancelado != 'S')
),

atendimentos_mes as (
    select
        id_usuario,
        id_unidade,
        date_trunc(data_atendimento, month) as mes_referencia,
        count(*) as qtd_atendimentos_total,
        countif(tipo_atendimento = 'Atendimento Técnico') as qtd_atendimentos_tecnico,
        countif(tipo_atendimento = 'Atendimento Recepção') as qtd_atendimentos_recepcao,
        countif(tipo_atendimento = 'Outros Atendimentos') as qtd_atendimentos_outros,
        max(case when tipo_atendimento = 'Atendimento Técnico' then data_atendimento end) as data_ultimo_atendimento_tecnico
    from atendimentos
    group by id_usuario, id_unidade, mes_referencia
),

-- Profissional de referência: profissional do último atendimento TÉCNICO do mês
-- (fallback: último atendimento do mês de qualquer tipo).
profissional_referencia as (
    select
        id_usuario,
        id_unidade,
        profissional,
        date_trunc(data_atendimento, month) as mes_referencia
    from atendimentos
    qualify row_number() over (
        partition by id_usuario, id_unidade, date_trunc(data_atendimento, month)
        order by (tipo_atendimento = 'Atendimento Técnico') desc, data_atendimento desc, id_atendimento desc
    ) = 1
),

-- Perfil cadastral do usuário
usuarios as (
    select
        du.id_usuario,
        du.nome,
        du.nome_social,
        du.cpf,
        du.filiacao_mae,
        du.data_nascimento,
        du.genero,
        du.orientacao_sexual,
        du.raca_cor,
        du.escolaridade_indice,
        du.flag_frequenta_escola,
        du.flag_recebe_beneficio,
        du.tipo_beneficio,
        du.beneficio,
        du.flag_cadunico,
        du.flag_deficiencia,
        du.tipo_deficiencia,
        du.flag_situacao_rua,
        du.atvd_remunerada,
        du.profissao,
        du.renda_ativa,
        ru.uf_nascimento,
        sm.serie_escolar
    from {{ ref('dim_usuarios') }} as du
    left join {{ ref('raw_usuarios') }} as ru on du.id_usuario = ru.id_paciente
    left join {{ ref('raw_usuarios_saude_mental') }} as sm on du.id_usuario = sm.id_paciente
),

-- Formulários do prontuário (codigo_abrangencia = 29, módulos família e usuário)
evolucoes_pai as (
    select
        e.id_evolucao,
        e.id_unidade,
        e.codigo_abrangencia,
        e.descricao_evolucao,
        e.data_evolucao,
        coalesce(du.id_usuario, e.id_paciente_familia) as id_paciente
    from {{ ref('fct_evolucoes') }} as e
    left join {{ ref('dim_usuarios') }} as du on e.id_usuario_sk = du.id_usuario_sk
    where
        e.codigo_abrangencia = 29
        and e.data_cancelamento is null
        and coalesce(du.id_usuario, e.id_paciente_familia) is not null
),

evolucoes_atendimento_social as (
    select
        e.id_evolucao,
        e.id_unidade,
        e.codigo_abrangencia,
        e.descricao_evolucao,
        e.data_evolucao,
        coalesce(du.id_usuario, e.id_paciente_familia) as id_paciente
    from {{ ref('fct_evolucoes') }} as e
    left join {{ ref('dim_usuarios') }} as du on e.id_usuario_sk = du.id_usuario_sk
    where
        e.codigo_abrangencia = 29
        and e.data_cancelamento is null
        and coalesce(du.id_usuario, e.id_paciente_familia) is not null
),

evolucoes_desligamento as (
    select
        e.id_evolucao,
        e.id_unidade,
        e.codigo_abrangencia,
        e.descricao_evolucao,
        e.data_evolucao,
        coalesce(du.id_usuario, e.id_paciente_familia) as id_paciente
    from {{ ref('fct_evolucoes') }} as e
    left join {{ ref('dim_usuarios') }} as du on e.id_usuario_sk = du.id_usuario_sk
    where
        e.codigo_abrangencia = 29
        and e.data_cancelamento is null
        and coalesce(du.id_usuario, e.id_paciente_familia) is not null
),

-- Motivo de permanência na rua: formulário 'Acolhimento' (não existe o campo
-- 'Motivo da ida às ruas' na fonte; usa 'Motivo do acolhimento' como fallback).
evolucoes_acolhimento as (
    select
        e.id_evolucao,
        e.codigo_abrangencia,
        e.descricao_evolucao,
        e.data_evolucao,
        du.id_usuario as id_paciente
    from {{ ref('fct_evolucoes') }} as e
    inner join {{ ref('dim_usuarios') }} as du on e.id_usuario_sk = du.id_usuario_sk
    where
        e.codigo_abrangencia = 1
        and e.data_cancelamento is null
        and e.origem_modulo = 'usuario'
),

evolucoes_documentacao as (
    select
        e.id_evolucao,
        e.codigo_abrangencia,
        e.descricao_evolucao,
        e.data_evolucao,
        coalesce(du.id_usuario, e.id_paciente_familia) as id_paciente
    from {{ ref('fct_evolucoes') }} as e
    left join {{ ref('dim_usuarios') }} as du on e.id_usuario_sk = du.id_usuario_sk
    where
        e.codigo_abrangencia = 24
        and e.data_cancelamento is null
        and coalesce(du.id_usuario, e.id_paciente_familia) is not null
),

-- Campo 'Data do atendimento:' do formulário PAI, por evolução
pai_data_atendimento as (
    select
        id_paciente,
        id_unidade,
        id_evolucao,
        max(case when label like 'Data do atendimento' then valor end) as data_atendimento
    from {{ extrair_campos_html_evolucao(
        source_relation = 'evolucoes_pai',
        id_cols = ['id_paciente', 'id_unidade', 'id_evolucao'],
        extra_where = 'codigo_abrangencia = 29'
    ) }}
    where titulo_formulario = 'Centro POP - Plano de Atendimento Individual (PAI)'
    group by id_paciente, id_unidade, id_evolucao
),

-- Inclusão no acompanhamento: campo 'Data do atendimento:' do formulário PAI
-- (primeira data preenchida pelo profissional; fallback: data da evolução).
pai_inclusao as (
    select
        e.id_paciente as id_usuario,
        e.id_unidade,
        coalesce(
            min(safe.parse_date('%d/%m/%Y', pv.data_atendimento)),
            min(e.data_evolucao)
        ) as data_inclusao_acompanhamento
    from evolucoes_pai as e
    left join pai_data_atendimento as pv
        on
            e.id_paciente = pv.id_paciente
            and e.id_unidade = pv.id_unidade
            and e.id_evolucao = pv.id_evolucao
    group by e.id_paciente, e.id_unidade
),

pai_form as (
    {{ extrair_formulario(
        source_relation = 'evolucoes_pai',
        group_cols = ['id_paciente', 'id_unidade'],
        codigo_abrangencia = 29,
        titulo_formulario = 'Centro POP - Plano de Atendimento Individual (PAI)',
        latest_by = 'data_evolucao',
        campos = [
            {'label': 'Data do atendimento', 'col': 'data_ultimo_pai', 'type': 'date'},
            {'label': 'Identificação das demandas apresentadas pelo usuário', 'col': 'demandas_pai', 'type': 'string'},
            {'label': 'Encaminhamentos', 'col': 'encaminhamentos_pai', 'type': 'string'}
        ]
    ) }}
),

atendimento_social_form as (
    {{ extrair_formulario(
        source_relation = 'evolucoes_atendimento_social',
        group_cols = ['id_paciente', 'id_unidade'],
        codigo_abrangencia = 29,
        titulo_formulario = 'Centro POP - Atendimento Social',
        latest_by = 'data_evolucao',
        campos = [
            {'label': 'Data do atendimento', 'col': 'data_ultimo_atendimento_social', 'type': 'date'},
            {'label': 'Forma de ingresso', 'col': 'forma_ingresso', 'type': 'string'},
            {'label': 'Demanda inicial', 'col': 'demanda_inicial', 'type': 'string'},
            {'label': 'Possui referências familiares', 'col': 'flag_possui_referencias_familiares', 'type': 'boolean'},
            {'label': 'Local de permanência na rua', 'col': 'local_permanencia_rua', 'type': 'string'},
            {'label': 'Local onde dorme', 'col': 'local_dorme', 'type': 'string'},
            {'label': 'Descrição das atividades de interesse', 'col': 'descricao_atividades_interesse', 'type': 'string'},
            {'label': 'Encaminhamentos', 'col': 'encaminhamentos_as', 'type': 'string'}
        ]
    ) }}
),

desligamento_form as (
    {{ extrair_formulario(
        source_relation = 'evolucoes_desligamento',
        group_cols = ['id_paciente', 'id_unidade'],
        codigo_abrangencia = 29,
        titulo_formulario = 'Centro POP - Desligamento PAI',
        latest_by = 'data_evolucao',
        campos = [
            {'label': 'Data do desligamento', 'col': 'data_desligamento', 'type': 'date'},
            {'label': 'Motivo do desligamento', 'col': 'motivo_desligamento', 'type': 'string'},
            {'label': 'Outros', 'col': 'motivo_desligamento_outros', 'type': 'string'}
        ]
    ) }}
),

acolhimento_form as (
    {{ extrair_formulario(
        source_relation = 'evolucoes_acolhimento',
        group_cols = ['id_paciente'],
        codigo_abrangencia = 1,
        titulo_formulario = 'Acolhimento',
        latest_by = 'data_evolucao',
        campos = [
            {'label': 'Motivo da ida às ruas', 'col': 'motivo_ida_ruas', 'type': 'string'},
            {'label': 'Motivo do acolhimento', 'col': 'motivo_acolhimento', 'type': 'string'},
            {'label': 'Outros', 'col': 'motivo_outros', 'type': 'string'}
        ]
    ) }}
),

documentacao_form as (
    {{ extrair_formulario(
        source_relation = 'evolucoes_documentacao',
        group_cols = ['id_paciente'],
        codigo_abrangencia = 24,
        titulo_formulario = 'Documentação Civil',
        latest_by = 'data_evolucao',
        flag_col = 'flag_registro_documentacao_civil',
        campos = []
    ) }}
),

-- Respostas do questionário estruturado 'Situação do Usuário' (módulo
-- HIST_PAC): pergunta 'Motivo da ida às ras:' (id_template=3, id_questao=18).
-- Campo inativo na fonte — sem registros hoje; mantém fallback do form Acolhimento.
questionario_situacao_usuario as (
    select
        q.id_prontuario as id_usuario,
        ql.data_resposta,
        ql.resposta as motivo_ida_ruas
    from {{ ref('raw_evolucoes_questionario') }} as q
    inner join {{ ref('raw_evolucoes_questionario_lista') }} as ql
        on q.id_evolucao = ql.id_evolucao
    where
        q.id_template = 3
        and ql.id_questao = 18
    qualify row_number() over (partition by q.id_prontuario order by ql.data_resposta desc) = 1
),

-- NIS via CadÚnico (documento_pessoa → identificacao_membro pelo CPF)
nis_cadunico as (
    select
        d.cpf,
        any_value(m.nis) as nis
    from {{ ref('raw_documento_pessoa') }} as d
    left join {{ ref('raw_identificacao_membro') }} as m on d.id_membro_familia = m.id_membro_familia
    where d.cpf is not null
    group by d.cpf
),

-- Oficinas/atividades coletivas do mês na unidade
oficinas as (
    select
        p.id_usuario,
        p.id_unidade,
        date_trunc(p.data_presenca, month) as mes_referencia,
        count(distinct p.id_atividade) as qtd_oficinas
    from {{ ref('fct_presencas_usuarios') }} as p
    inner join centro_pop as c on p.id_unidade = c.id_unidade
    group by p.id_usuario, p.id_unidade, mes_referencia
),

final as (
    select
        am.id_usuario,
        am.id_unidade,
        c.nome_unidade as nome_centro_pop,
        am.mes_referencia,
        pr.profissional as profissional_referencia,
        u.nome as nome_usuario,
        u.nome_social,
        not coalesce(
            pi.data_inclusao_acompanhamento is not null
            and pi.data_inclusao_acompanhamento < date_add(am.mes_referencia, interval 1 month),
            false
        ) as flag_atendido_pontualmente,
        coalesce(
            pi.data_inclusao_acompanhamento is not null
            and pi.data_inclusao_acompanhamento < date_add(am.mes_referencia, interval 1 month),
            false
        ) as flag_inserido_acompanhamento,
        pi.data_inclusao_acompanhamento,
        coalesce(
            pi.data_inclusao_acompanhamento is not null
            and pi.data_inclusao_acompanhamento < date_add(am.mes_referencia, interval 1 month),
            false
        ) as flag_possui_plano_individual,
        am.data_ultimo_atendimento_tecnico,
        am.qtd_atendimentos_total,
        am.qtd_atendimentos_tecnico,
        am.qtd_atendimentos_recepcao,
        am.qtd_atendimentos_outros,
        coalesce(
            nullif(q.motivo_ida_ruas, 'undefined'),
            nullif(acf.motivo_ida_ruas, 'undefined'),
            nullif(acf.motivo_acolhimento, 'undefined')
        ) as motivo_principal_permanencia_rua,
        nullif(acf.motivo_outros, 'undefined') as motivo_secundario_permanencia_rua,
        u.data_nascimento,
        cast(date_diff(date_add(am.mes_referencia, interval 1 month) - interval 1 day, u.data_nascimento, year) as int64) as idade,
        u.filiacao_mae,
        doc.flag_registro_documentacao_civil,
        u.genero,
        u.orientacao_sexual,
        u.raca_cor,
        u.uf_nascimento as naturalidade,
        case u.flag_frequenta_escola
            when 'S' then 'Sim'
            when 'N' then 'Não'
            else 'Não Informado'
        end as flag_estuda,
        u.serie_escolar as ano_cursando,
        u.escolaridade_indice as nivel_escolaridade,
        u.flag_deficiencia,
        u.tipo_deficiencia,
        null as flag_uso_substancias_psicoativas,
        null as flag_problema_saude,
        null as flag_acompanhamento_saude,
        asf.flag_possui_referencias_familiares as flag_possui_vinculo_familiar,
        null as territorio_referencia_familia,
        null as flag_possibilidade_reinsercao_familiar,
        u.atvd_remunerada as flag_exerce_atividade_renda,
        u.profissao as atividade_renda_qual,
        u.renda_ativa as valor_renda,
        null as flag_empregabilidade_imediata,
        null as capacidade_habilidades,
        null as flag_interesse_curso,
        case u.flag_recebe_beneficio
            when 'S' then 'Sim'
            when 'N' then 'Não'
            else 'Não Informado'
        end as flag_possui_beneficio,
        u.tipo_beneficio,
        u.beneficio,
        case u.flag_cadunico
            when 'S' then 'Sim'
            when 'N' then 'Não'
            else 'Não Informado'
        end as flag_possui_cadunico,
        null as flag_cadastro_atualizado,
        n.nis as nis_usuario,
        coalesce(ofc.qtd_oficinas > 0, false) as flag_participacao_oficinas,
        ofc.qtd_oficinas as qtd_oficinas_participadas_mes,
        array(
            select x
            from
                unnest([
                    struct('Centro POP - Plano de Atendimento Individual (PAI)' as origem, nullif(pf.demandas_pai, 'undefined') as demanda),
                    struct('Centro POP - Atendimento Social' as origem, nullif(asf.demanda_inicial, 'undefined') as demanda)
                ]) as x
            where x.demanda is not null
        ) as demandas,
        array(
            select x
            from
                unnest([
                    struct('Centro POP - Plano de Atendimento Individual (PAI)' as origem, nullif(pf.encaminhamentos_pai, 'undefined') as encaminhamento),
                    struct('Centro POP - Atendimento Social' as origem, nullif(asf.encaminhamentos_as, 'undefined') as encaminhamento)
                ]) as x
            where x.encaminhamento is not null
        ) as encaminhamentos,
        null as resultado_acesso,
        null as resultado_descricao,
        df.data_desligamento,
        array_to_string(
            [
                nullif(df.motivo_desligamento, 'undefined'),
                nullif(df.motivo_desligamento_outros, 'undefined')
            ],
            '; '
        ) as motivo_desligamento,
        null as observacoes,
        u.flag_situacao_rua,
        {{ extrair_ultima_atualizacao('raw_configuracoes_sistema') }} as ultima_atualizacao
    from atendimentos_mes as am
    inner join centro_pop as c on am.id_unidade = c.id_unidade
    left join profissional_referencia as pr
        on
            am.id_usuario = pr.id_usuario
            and am.id_unidade = pr.id_unidade
            and am.mes_referencia = pr.mes_referencia
    left join usuarios as u on am.id_usuario = u.id_usuario
    left join pai_inclusao as pi
        on
            am.id_usuario = pi.id_usuario
            and am.id_unidade = pi.id_unidade
    left join pai_form as pf
        on
            am.id_usuario = pf.id_paciente
            and am.id_unidade = pf.id_unidade
    left join atendimento_social_form as asf
        on
            am.id_usuario = asf.id_paciente
            and am.id_unidade = asf.id_unidade
    left join desligamento_form as df
        on
            am.id_usuario = df.id_paciente
            and am.id_unidade = df.id_unidade
    left join acolhimento_form as acf on am.id_usuario = acf.id_paciente
    left join questionario_situacao_usuario as q on am.id_usuario = q.id_usuario
    left join documentacao_form as doc on am.id_usuario = doc.id_paciente
    left join nis_cadunico as n on u.cpf = n.cpf
    left join oficinas as ofc
        on
            am.id_usuario = ofc.id_usuario
            and am.id_unidade = ofc.id_unidade
            and am.mes_referencia = ofc.mes_referencia
)

select * from final
