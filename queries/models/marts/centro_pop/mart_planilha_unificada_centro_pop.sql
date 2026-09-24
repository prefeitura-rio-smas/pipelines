{{ config(tags = ['daily']) }}
-- Mart: Planilha Unificada Centro POP
-- Granularidade: 1 linha por id_atendimento não cancelado em unidade Centro POP.
-- Contagens de atendimentos e oficinas são contextos mensais repetidos em cada
-- atendimento do mesmo usuário × unidade × mês.
--
-- Fontes:
--   - dim_usuarios / raw_usuarios / raw_usuarios_saude_mental: perfil cadastral;
--   - fct_atendimentos + dim_profissionais: atendimentos e profissionais associados;
--   - fct_evolucoes (codigo_abrangencia = 29) + macro extrair_formulario:
--     formulários 'Centro POP - Plano de Atendimento Individual (PAI)',
--     'Centro POP - Atendimento Social' e 'Centro POP - Desligamento PAI';
--   - fct_evolucoes (codigo_abrangencia = 1, form 'Acolhimento'): motivos de
--     permanência na rua (não existe o campo 'Motivo da ida às ruas' na fonte);
--   - fct_evolucoes (codigo_abrangencia = 24, form 'Documentação Civil'):
--     existência de formulário extraível, sem inferir demanda ou posse documental;
--   - cadunico (raw_documento_pessoa + raw_identificacao_membro via CPF): NIS;
--   - fct_presencas_usuarios: oficinas/atividades coletivas no mês.

with centro_pop as (
    select
        id_unidade,
        max(nome_unidade) as nome_unidade
    from {{ ref('dim_unidades') }}
    where tipo_unidade = 'Centro POP'
    group by id_unidade
),

-- O fato tem uma linha por atendimento × profissional compartilhado.
atendimentos_candidatos as (
    select
        a.id_usuario,
        a.id_unidade,
        a.id_atendimento,
        a.id_atendimento_modulo,
        a.id_profissional,
        safe_cast(a.data_atendimento as date) as data_atendimento,
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
    inner join centro_pop as c on a.id_unidade = c.id_unidade
    left join {{ ref('dim_profissionais') }} as dp on a.id_profissional_sk = dp.id_profissional_sk
    where (a.flag_cancelado is null or a.flag_cancelado != 'S')
),

-- Reagrega profissionais compartilhados para manter uma linha por atendimento.
-- A classificação conserva a precedência: técnico, recepção e outros.
atendimentos as (
    select
        id_atendimento,
        min(id_usuario) as id_usuario,
        min(id_unidade) as id_unidade,
        min(data_atendimento) as data_atendimento,
        max(nome_atendimento) as nome_atendimento,
        array_agg(distinct profissional ignore nulls order by profissional) as profissionais_atendimento,
        case
            when countif(tipo_atendimento = 'Atendimento Técnico') > 0 then 'Atendimento Técnico'
            when countif(tipo_atendimento = 'Atendimento Recepção') > 0 then 'Atendimento Recepção'
            else 'Outros Atendimentos'
        end as tipo_atendimento
    from atendimentos_candidatos
    group by id_atendimento
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
        max(data_atendimento) as data_ultimo_atendimento_mes
    from atendimentos
    group by id_usuario, id_unidade, mes_referencia
),

-- Família do usuário via Cadastro de Famílias (1 família por usuário responsável)
familia_usuario as (
    select
        id_usuario_responsavel as id_usuario,
        id_familia
    from {{ ref('dim_familias') }}
    qualify row_number() over (
        partition by id_usuario_responsavel
        order by data_ultima_modificacao desc
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
        du.grau_dependencia,
        du.flag_saude_mental_comprometida,
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

-- Situação de Saúde: formulário do prontuário (codigo_abrangencia = 1),
-- registrado no módulo do usuário; join por usuário sem filtro de unidade,
-- pois o formulário pode ter sido preenchido em outra unidade.
evolucoes_saude as (
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

-- Primeira inclusão PAI conhecida até a data de cada atendimento. A data
-- informada no formulário tem precedência; a data da evolução é fallback.
pai_inclusao_atendimento as (
    select
        a.id_atendimento,
        coalesce(
            min(safe.parse_date('%d/%m/%Y', pv.data_atendimento)),
            min(safe_cast(e.data_evolucao as date))
        ) as data_inclusao_acompanhamento
    from atendimentos as a
    inner join evolucoes_pai as e
        on
            a.id_usuario = e.id_paciente
            and a.id_unidade = e.id_unidade
            and safe_cast(e.data_evolucao as date) <= a.data_atendimento
    left join pai_data_atendimento as pv
        on
            e.id_paciente = pv.id_paciente
            and e.id_unidade = pv.id_unidade
            and e.id_evolucao = pv.id_evolucao
    where coalesce(safe.parse_date('%d/%m/%Y', pv.data_atendimento), safe_cast(e.data_evolucao as date)) <= a.data_atendimento
    group by a.id_atendimento
),

pai_form as (
    {{ extrair_formulario(
        source_relation = 'evolucoes_pai',
        group_cols = ['id_paciente', 'id_unidade', 'id_evolucao', 'data_evolucao'],
        codigo_abrangencia = 29,
        titulo_formulario = 'Centro POP - Plano de Atendimento Individual (PAI)',
        campos = [
            {'label': 'Data do atendimento', 'col': 'data_ultimo_pai', 'type': 'date'},
            {'label': 'Identificação das demandas apresentadas pelo usuário', 'col': 'demandas_pai', 'type': 'string'},
            {'label': 'Encaminhamentos%', 'col': 'encaminhamentos_pai', 'type': 'array_agg'}
        ]
    ) }}
),

atendimento_social_form as (
    {{ extrair_formulario(
        source_relation = 'evolucoes_atendimento_social',
        group_cols = ['id_paciente', 'id_unidade', 'id_evolucao', 'data_evolucao'],
        codigo_abrangencia = 29,
        titulo_formulario = 'Centro POP - Atendimento Social',
        campos = [
            {'label': 'Data do atendimento', 'col': 'data_ultimo_atendimento_social', 'type': 'date'},
            {'label': 'Forma de ingresso', 'col': 'forma_ingresso', 'type': 'string'},
            {'label': 'Demanda inicial', 'col': 'demanda_inicial', 'type': 'string'},
            {'label': 'Possui referências familiares', 'col': 'flag_possui_referencias_familiares', 'type': 'boolean'},
            {'label': 'Local de permanência na rua', 'col': 'local_permanencia_rua', 'type': 'string'},
            {'label': 'Local onde dorme', 'col': 'local_dorme', 'type': 'string'},
            {'label': 'Descrição das atividades de interesse', 'col': 'descricao_atividades_interesse', 'type': 'string'},
            {'label': 'Encaminhamentos%', 'col': 'encaminhamentos_as', 'type': 'array_agg'}
        ]
    ) }}
),

desligamento_form as (
    {{ extrair_formulario(
        source_relation = 'evolucoes_desligamento',
        group_cols = ['id_paciente', 'id_unidade', 'id_evolucao', 'data_evolucao'],
        codigo_abrangencia = 29,
        titulo_formulario = 'Centro POP - Desligamento PAI',
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
        group_cols = ['id_paciente', 'id_evolucao', 'data_evolucao'],
        codigo_abrangencia = 1,
        titulo_formulario = 'Acolhimento',
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
        group_cols = ['id_paciente', 'id_evolucao', 'data_evolucao'],
        codigo_abrangencia = 24,
        titulo_formulario = 'Documentação Civil',
        campos = [
            {'label': '%', 'col': 'flag_registro_formulario_documentacao_civil', 'type': 'exists'}
        ]
    ) }}
),

-- Saúde: formulário 'Situação de Saúde' (primário) com fallback para o
-- cadastro (dim_usuarios: grau_dependencia e flag_saude_mental_comprometida).
situacao_saude as (
    {{ extrair_formulario(
        source_relation = 'evolucoes_saude',
        group_cols = ['id_paciente', 'id_evolucao', 'data_evolucao'],
        codigo_abrangencia = 1,
        titulo_formulario = 'Situação de Saúde',
        campos = [
            {'label': 'Faz uso de substâncias psicoativas?', 'col': 'uso_substancias', 'type': 'string'},
            {'label': 'Situação', 'col': 'situacao_saude', 'type': 'string'},
            {'label': 'Local onde faz tratamento', 'col': 'local_tratamento', 'type': 'string'}
        ]
    ) }}
),

-- Respostas do questionário estruturado 'Situação do Usuário' (HIST_PAC).
-- A seleção temporal é feita por atendimento abaixo.
questionario_situacao_usuario as (
    select
        q.id_prontuario as id_usuario,
        q.id_evolucao,
        ql.data_resposta,
        ql.resposta as motivo_ida_ruas
    from {{ ref('raw_evolucoes_questionario') }} as q
    inner join {{ ref('raw_evolucoes_questionario_lista') }} as ql
        on q.id_evolucao = ql.id_evolucao
    where
        q.id_template = 3
        and ql.id_questao = 18
),

-- Seleciona o formulário mais recente registrado até cada atendimento. Formulários
-- posteriores não retroagem para atendimentos anteriores.
pai_form_atendimento as (
    select a.id_atendimento, pf.demandas_pai, pf.encaminhamentos_pai
    from atendimentos as a
    inner join pai_form as pf
        on
            a.id_usuario = pf.id_paciente
            and a.id_unidade = pf.id_unidade
            and safe_cast(pf.data_evolucao as date) <= a.data_atendimento
    qualify row_number() over (
        partition by a.id_atendimento
        order by pf.data_evolucao desc, pf.id_evolucao desc
    ) = 1
),

atendimento_social_form_atendimento as (
    select a.id_atendimento, asf.demanda_inicial, asf.flag_possui_referencias_familiares, asf.encaminhamentos_as
    from atendimentos as a
    inner join atendimento_social_form as asf
        on
            a.id_usuario = asf.id_paciente
            and a.id_unidade = asf.id_unidade
            and safe_cast(asf.data_evolucao as date) <= a.data_atendimento
    qualify row_number() over (
        partition by a.id_atendimento
        order by asf.data_evolucao desc, asf.id_evolucao desc
    ) = 1
),

desligamento_form_atendimento as (
    select a.id_atendimento, df.data_desligamento, df.motivo_desligamento, df.motivo_desligamento_outros
    from atendimentos as a
    inner join desligamento_form as df
        on
            a.id_usuario = df.id_paciente
            and a.id_unidade = df.id_unidade
            and safe_cast(df.data_evolucao as date) <= a.data_atendimento
    qualify row_number() over (
        partition by a.id_atendimento
        order by df.data_evolucao desc, df.id_evolucao desc
    ) = 1
),

acolhimento_form_atendimento as (
    select a.id_atendimento, acf.motivo_ida_ruas, acf.motivo_acolhimento, acf.motivo_outros
    from atendimentos as a
    inner join acolhimento_form as acf
        on a.id_usuario = acf.id_paciente and safe_cast(acf.data_evolucao as date) <= a.data_atendimento
    qualify row_number() over (
        partition by a.id_atendimento
        order by acf.data_evolucao desc, acf.id_evolucao desc
    ) = 1
),

documentacao_form_atendimento as (
    select a.id_atendimento, doc.flag_registro_formulario_documentacao_civil
    from atendimentos as a
    inner join documentacao_form as doc
        on a.id_usuario = doc.id_paciente and safe_cast(doc.data_evolucao as date) <= a.data_atendimento
    qualify row_number() over (
        partition by a.id_atendimento
        order by doc.data_evolucao desc, doc.id_evolucao desc
    ) = 1
),

situacao_saude_atendimento as (
    select a.id_atendimento, ss.uso_substancias, ss.situacao_saude, ss.local_tratamento
    from atendimentos as a
    inner join situacao_saude as ss
        on a.id_usuario = ss.id_paciente and safe_cast(ss.data_evolucao as date) <= a.data_atendimento
    qualify row_number() over (
        partition by a.id_atendimento
        order by ss.data_evolucao desc, ss.id_evolucao desc
    ) = 1
),

questionario_situacao_usuario_atendimento as (
    select a.id_atendimento, q.motivo_ida_ruas
    from atendimentos as a
    inner join questionario_situacao_usuario as q
        on a.id_usuario = q.id_usuario and safe_cast(q.data_resposta as date) <= a.data_atendimento
    qualify row_number() over (
        partition by a.id_atendimento
        order by q.data_resposta desc, q.id_evolucao desc
    ) = 1
),

-- NIS via CadÚnico. A dimensão de membro expõe apenas id_membro_familia,
-- portanto o vínculo segue essa chave e a saída é reduzida a uma linha/CPF.
nis_cadunico as (
    select
        nullif(regexp_replace(coalesce(d.cpf, ''), r'[^0-9]', ''), '') as cpf_normalizado,
        any_value(m.nis) as nis
    from {{ ref('raw_documento_pessoa') }} as d
    left join {{ ref('raw_identificacao_membro') }} as m on d.id_membro_familia = m.id_membro_familia
    where nullif(regexp_replace(coalesce(d.cpf, ''), r'[^0-9]', ''), '') is not null
    group by cpf_normalizado
),

-- Documentos do CadÚnico ligados ao prontuário por CPF normalizado. A família
-- do CadÚnico é uma chave distinta da família do prontuário; só é usada para
-- data de atualização quando o CPF aponta para uma única família CadÚnico.
documentos_cadunico as (
    select
        nullif(regexp_replace(coalesce(cpf, ''), r'[^0-9]', ''), '') as cpf_normalizado,
        count(distinct safe_cast(trim(id_familia) as int64)) as qtd_familias_cadunico,
        if(
            count(distinct safe_cast(trim(id_familia) as int64)) = 1,
            max(safe_cast(trim(id_familia) as int64)),
            null
        ) as id_familia_cadunico,
        if(
            count(distinct nullif(regexp_replace(trim(rg), r'^0+', ''), '')) = 1,
            max(nullif(regexp_replace(trim(rg), r'^0+', ''), '')),
            null
        ) as numero_rg,
        if(
            count(distinct nullif(regexp_replace(trim(rg), r'^0+', ''), '')) = 1,
            'Sim',
            'Não Informado'
        ) as flag_possui_rg,
        if(logical_or(safe_cast(id_certidao_civil as int64) = 1), 'Sim', 'Não Informado') as flag_possui_certidao_nascimento,
        if(
            count(distinct if(
                safe_cast(id_certidao_civil as int64) = 1,
                nullif(trim(id_termi_matricula_certidao), ''),
                null
            )) = 1,
            max(if(
                safe_cast(id_certidao_civil as int64) = 1,
                nullif(trim(id_termi_matricula_certidao), ''),
                null
            )),
            null
        ) as numero_certidao_nascimento
    from {{ ref('raw_documento_pessoa') }}
    where nullif(regexp_replace(coalesce(cpf, ''), r'[^0-9]', ''), '') is not null
    group by cpf_normalizado
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

-- Última atualização por família CadÚnico. id_familia é STRING na fonte e
-- data_atualizacao já é DATE na camada raw.
cadunico_atualizacao as (
    select
        safe_cast(trim(id_familia) as int64) as id_familia_cadunico,
        max(data_atualizacao) as data_atualizacao
    from {{ ref('raw_identificacao_controle') }}
    where safe_cast(trim(id_familia) as int64) is not null
    group by id_familia_cadunico
),

final as (
    select
        a.id_usuario,
        fam.id_familia,
        a.id_unidade,
        c.nome_unidade as nome_centro_pop,
        date_trunc(a.data_atendimento, month) as mes_referencia,
        a.id_atendimento,
        a.data_atendimento,
        a.nome_atendimento,
        a.tipo_atendimento,
        a.profissionais_atendimento,
        u.nome as nome_usuario,
        u.nome_social,
        {{ map_flag_boolean('not coalesce(
            pi.data_inclusao_acompanhamento is not null
            and pi.data_inclusao_acompanhamento <= a.data_atendimento,
            false
        )') }} as flag_atendido_pontualmente,
        {{ map_flag_boolean('coalesce(
            pi.data_inclusao_acompanhamento is not null
            and pi.data_inclusao_acompanhamento <= a.data_atendimento,
            false
        )') }} as flag_inserido_acompanhamento,
        pi.data_inclusao_acompanhamento,
        {{ map_flag_boolean('coalesce(
            pi.data_inclusao_acompanhamento is not null
            and pi.data_inclusao_acompanhamento <= a.data_atendimento,
            false
        )') }} as flag_possui_plano_individual,
        am.data_ultimo_atendimento_mes,
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
        safe_cast(date_diff(a.data_atendimento, u.data_nascimento, year) as int64) as idade,
        u.filiacao_mae,
        {{ map_flag_boolean('doc.flag_registro_formulario_documentacao_civil') }} as flag_registro_formulario_documentacao_civil,
        case
            when nullif(trim(u.cpf), '') is not null then 'Sim'
            else 'Não Informado'
        end as flag_possui_cpf,
        nullif(trim(u.cpf), '') as numero_cpf,
        coalesce(dc.flag_possui_rg, 'Não Informado') as flag_possui_rg,
        dc.numero_rg,
        coalesce(dc.flag_possui_certidao_nascimento, 'Não Informado') as flag_possui_certidao_nascimento,
        dc.numero_certidao_nascimento,
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
        case
            when upper(trim(u.flag_deficiencia)) in ('S', 'SIM') then 'Sim'
            when upper(trim(u.flag_deficiencia)) in ('N', 'NAO', 'NÃO') then 'Não'
            else 'Não Informado'
        end as flag_deficiencia,
        {{ map_coluna_tipo_deficiencia('u.tipo_deficiencia') }} as tipo_deficiencia,
        case
            when ss.uso_substancias = 'S' then 'Sim'
            when ss.uso_substancias = 'N' then 'Não'
            when u.grau_dependencia in ('1', '2', '3') then 'Sim'
            else 'Não Informado'
        end as flag_uso_substancias_psicoativas,
        case
            when nullif(ss.situacao_saude, 'undefined') is not null then 'Sim'
            when u.flag_saude_mental_comprometida not in ('N', '') then 'Sim'
            when u.flag_saude_mental_comprometida = 'N' then 'Não'
            else 'Não Informado'
        end as flag_problema_saude,
        case
            when nullif(ss.local_tratamento, 'undefined') is not null then 'Sim'
            else 'Não Informado'
        end as flag_acompanhamento_saude,
        {{ map_flag_boolean('asf.flag_possui_referencias_familiares') }} as flag_possui_vinculo_familiar,
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
        ic.data_atualizacao as data_atualizacao_cadunico,
        n.nis as nis_usuario,
        {{ map_flag_boolean('coalesce(ofc.qtd_oficinas > 0, false)') }} as flag_participacao_oficinas,
        coalesce(ofc.qtd_oficinas, 0) as qtd_oficinas_participadas_mes,
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
            select as struct
                x.origem,
                e as encaminhamento
            from
                unnest([
                    struct(
                        'Centro POP - Plano de Atendimento Individual (PAI)' as origem,
                        pf.encaminhamentos_pai as encaminhamento
                    ),
                    struct(
                        'Centro POP - Atendimento Social' as origem,
                        asf.encaminhamentos_as as encaminhamento
                    )
                ]) as x
            cross join unnest(coalesce(x.encaminhamento, [])) as e
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
    from atendimentos as a
    inner join centro_pop as c on a.id_unidade = c.id_unidade
    left join atendimentos_mes as am
        on
            a.id_usuario = am.id_usuario
            and a.id_unidade = am.id_unidade
            and date_trunc(a.data_atendimento, month) = am.mes_referencia
    left join usuarios as u on a.id_usuario = u.id_usuario
    left join familia_usuario as fam on a.id_usuario = fam.id_usuario
    left join pai_inclusao_atendimento as pi on a.id_atendimento = pi.id_atendimento
    left join pai_form_atendimento as pf on a.id_atendimento = pf.id_atendimento
    left join atendimento_social_form_atendimento as asf on a.id_atendimento = asf.id_atendimento
    left join desligamento_form_atendimento as df on a.id_atendimento = df.id_atendimento
    left join acolhimento_form_atendimento as acf on a.id_atendimento = acf.id_atendimento
    left join situacao_saude_atendimento as ss on a.id_atendimento = ss.id_atendimento
    left join questionario_situacao_usuario_atendimento as q on a.id_atendimento = q.id_atendimento
    left join documentacao_form_atendimento as doc on a.id_atendimento = doc.id_atendimento
    left join documentos_cadunico as dc
        on nullif(regexp_replace(coalesce(u.cpf, ''), r'[^0-9]', ''), '') = dc.cpf_normalizado
    left join nis_cadunico as n
        on nullif(regexp_replace(coalesce(u.cpf, ''), r'[^0-9]', ''), '') = n.cpf_normalizado
    left join cadunico_atualizacao as ic
        on dc.id_familia_cadunico = ic.id_familia_cadunico
    left join oficinas as ofc
        on
            a.id_usuario = ofc.id_usuario
            and a.id_unidade = ofc.id_unidade
            and date_trunc(a.data_atendimento, month) = ofc.mes_referencia
    -- Preserva atendimentos sem cadastro dimensional; remove usuários de teste
    -- quando o nome cadastral está disponível.
    where u.nome is null or lower(u.nome) not like '%teste%'
)
select * from final
